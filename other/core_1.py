from collections import defaultdict
import pandas as pd
from typing import Dict, Set, Any


class IDTracker:
    def __init__(self):
        # Изменяем структуру: column -> value -> dict(value_position -> set of ids)
        self.tracking = defaultdict(lambda: defaultdict(dict))

    def track_operation(
        self,
        operation: str,
        source_ids: Set[str],
        result_value: Any,
        column: str,
        position: Any = None,
    ):
        """
        source_ids: множество ID источников
        result_value: значение после операции
        position: позиция значения (например, категория для группировки)
        """
        if operation == "initial":
            # Для начальных значений создаем новый набор ID с позицией
            if position not in self.tracking[column][result_value]:
                self.tracking[column][result_value][position] = set()
            self.tracking[column][result_value][position].update(source_ids)
        elif operation == "aggregate":
            # Для агрегированных значений добавляем ID с новой позицией
            self.tracking[column][result_value][position] = source_ids

    def get_source_ids(self, value: Any, column: str, position: Any = None) -> Set[str]:
        """Получаем ID для значения в конкретной позиции"""
        if position is not None and value in self.tracking[column]:
            return self.tracking[column][value].get(position, set())
        return set()


def process_data_with_tracking(data: list, value_mapping: Dict[str, float], group_cols: list):
    tracker = IDTracker()
    df = pd.DataFrame(data)

    # Сохраняем исходные ID и их значения
    for col in df.columns:
        for idx, row in df.iterrows():
            value = row[col]
            if str(value) in value_mapping:
                actual_value = value_mapping[str(value)]
                # Сохраняем для каждой колонки группировки
                for group_col in group_cols:
                    position = f"{group_col}:{row.get(group_col)}"  # Добавляем префикс колонки
                    tracker.track_operation(
                        "initial", {str(value)}, actual_value, col, position
                    )
                df.at[idx, col] = actual_value

    return df, tracker


def tracked_groupby_sum(df: pd.DataFrame, group_col: str, sum_col: str, tracker: IDTracker):
    """Группировка с отслеживанием исходных ID"""
    result = df.groupby(group_col)[sum_col].sum().reset_index()

    # Для каждой группы собираем ID только из этой группы
    for _, row in result.iterrows():
        group_value = row[group_col]
        sum_value = row[sum_col]

        # Находим все строки для данной группы
        group_rows = df[df[group_col] == group_value]

        # Собираем ID только для текущей группы
        group_source_ids = set()
        for _, group_row in group_rows.iterrows():
            value = group_row[sum_col]
            # Получаем ID только для текущей категории, используя префикс
            position = f"{group_col}:{group_value}"
            value_ids = tracker.get_source_ids(value, sum_col, position)
            group_source_ids.update(value_ids)

        # Сохраняем связь между суммой и ID группы
        tracker.track_operation(
            "aggregate", group_source_ids, sum_value, sum_col, f"{group_col}:{group_value}"
        )

    return result


# Тест:
test_data = [
    {"amount": "id:001", "category": "A", "kat": "q1"},
    {"amount": "id:002", "category": "A", "kat": "q2"},
    {"amount": "id:003", "category": "B", "kat": "q1"},
    {"amount": "id:004", "category": "B", "kat": "q2"},
    {"amount": "id:005", "category": "A", "kat": "q1"},
    {"amount": "id:006", "category": "С", "kat": "q2"},
    {"amount": "id:007", "category": "С", "kat": "q1"},
    {"amount": "id:008", "category": "С", "kat": "q2"},
    {"amount": "id:009", "category": "С", "kat": "q1"},
]

test_values = {
    "id:001": 100.0,
    "id:002": 100.0,
    "id:003": 100.0,
    "id:004": 100.0,
    "id:005": 100.0,
    "id:006": 100.0,
    "id:007": 100.0,
    "id:008": 100.0,
    "id:009": 100.0,
}

# Использование:
# Указываем все колонки, по которым будем группировать
df, tracker = process_data_with_tracking(test_data, test_values, ["category", "kat"])

# Первая группировка по category
grouped_by_category = tracked_groupby_sum(df, "category", "amount", tracker)

# Вторая группировка по kat (с тем же df и tracker)
grouped_by_kat = tracked_groupby_sum(grouped_by_category, "kat", "amount", tracker)

# Проверки для kat
print("\nГруппировка по kat:")
for kat in ["q1", "q2"]:
    sum_value = grouped_by_kat.loc[grouped_by_kat["kat"] == kat, "amount"].iloc[0]
    ids = tracker.get_source_ids(sum_value, "amount", f"kat:{kat}")
    print(f"Значение {sum_value} для kat {kat} собрано из ID: {ids}")