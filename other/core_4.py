import pandas as pd

from core_2 import MockDBConnector


# Подход 1: Использование attrs
def create_tracked_df_with_attrs(data_with_ids, db_connector):
    # Создаем основной DataFrame с значениями
    rows = []
    sources_dict = {}  # Словарь для хранения source_ids

    for row in data_with_ids:
        values_row = {}
        for col, id_value in row.items():
            values_row[col] = db_connector.get_value(id_value)
            # Сохраняем source_id для каждой ячейки
            sources_dict[(len(rows), col)] = {id_value}
        rows.append(values_row)

    df = pd.DataFrame(rows)
    # Сохраняем source_ids в атрибутах
    df.attrs["sources"] = sources_dict
    return df


# Подход 2: Параллельный DataFrame
def create_tracked_dfs(data_with_ids, db_connector):
    # Создаем два DataFrame: для значений и для source_ids
    values_rows = []
    sources_rows = []

    for row in data_with_ids:
        values_row = {}
        sources_row = {}
        for col, id_value in row.items():
            values_row[col] = db_connector.get_value(id_value)
            sources_row[col] = {id_value}
        values_rows.append(values_row)
        sources_rows.append(sources_row)

    values_df = pd.DataFrame(values_rows)
    sources_df = pd.DataFrame(sources_rows)
    return values_df, sources_df


# Пример использования:
connector = MockDBConnector()
test_data = [
    {"product": "id_1", "sales": "id_5", "category": "id_9"},
    {"product": "id_2", "sales": "id_6", "category": "id_9"},
    {"product": "id_3", "sales": "id_7", "category": "id_10"},
    {"product": "id_4", "sales": "id_8", "category": "id_11"},
]

# Используем подход с attrs
df = create_tracked_df_with_attrs(test_data, connector)

# Группируем и суммируем
grouped = df.groupby("category")["sales"].sum()

# Обновляем source_ids после группировки
new_sources = {}
for category in grouped.index:
    # Находим все строки с данной категорией
    rows = df[df["category"] == category].index
    # Объединяем source_ids для всех ячеек sales в этих строках
    category_sources = set()
    for row in rows:
        category_sources.update(df.attrs["sources"][(row, "sales")])
    new_sources[category] = category_sources

# Создаем новый DataFrame с результатами и source_ids
result_df = pd.DataFrame(grouped)
result_df.attrs["sources"] = new_sources

# Вывод результатов
for category, value in result_df.iterrows():
    print(f"\nCategory: {category}")
    print(f"Total sales: {value['sales']}")
    print(f"Source IDs: {result_df.attrs['sources'][category]}")
