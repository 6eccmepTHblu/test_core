class TrackedValue:
    def __init__(self, value, source_ids):
        self.value = value
        self.source_ids = set(source_ids)

    def __add__(self, other):
        return TrackedValue(
            self.value + other.value, self.source_ids | other.source_ids
        )

    def __repr__(self):
        return f"Value({self.value})[from {len(self.source_ids)} sources]"


def create_tracked_table(data_with_ids, db_connector):
    """
    Преобразует таблицу с ID в таблицу с отслеживаемыми значениями
    """
    result = []
    for row in data_with_ids:
        tracked_row = {}
        for col, id_value in row.items():
            real_value = db_connector.get_value(id_value)
            tracked_row[col] = TrackedValue(real_value, [id_value])
        result.append(tracked_row)
    return result


class MockDBConnector:
    def __init__(self):
        """
        Инициализация тестовых данных
        Создаем словари для хранения значений и метаданных
        """
        # Симулируем таблицу с данными продуктов
        self.products = {
            "id_1": "Laptop",
            "id_2": "Phone",
            "id_3": "Tablet",
            "id_4": "Monitor"
        }

        # Симулируем таблицу с данными продаж
        self.sales = {
            "id_5": 1500.0,
            "id_6": 800.0,
            "id_7": 600.0,
            "id_8": 1200.0
        }

        # Симулируем таблицу с категориями
        self.categories = {
            "id_9": "Electronics",
            "id_10": "Accessories",
            "id_11": "Gadgets"
        }

        # Метаданные для каждого ID
        self.metadata = {
            "id_1": {"filename": "products.csv", "row": 1, "created_at": "2024-02-17"},
            "id_2": {"filename": "products.csv", "row": 2, "created_at": "2024-02-17"},
            "id_3": {"filename": "products.csv", "row": 3, "created_at": "2024-02-17"},
            "id_4": {"filename": "products.csv", "row": 4, "created_at": "2024-02-17"},
            "id_5": {"filename": "sales.csv", "row": 1, "created_at": "2024-02-17"},
            "id_6": {"filename": "sales.csv", "row": 2, "created_at": "2024-02-17"},
            "id_7": {"filename": "sales.csv", "row": 3, "created_at": "2024-02-17"},
            "id_8": {"filename": "sales.csv", "row": 4, "created_at": "2024-02-17"},
            "id_9": {"filename": "categories.csv", "row": 1, "created_at": "2024-02-17"},
            "id_10": {"filename": "categories.csv", "row": 2, "created_at": "2024-02-17"},
            "id_11": {"filename": "categories.csv", "row": 3, "created_at": "2024-02-17"}
        }

    def get_value(self, id):
        """
        Получает значение по ID из соответствующей таблицы
        """
        if id in self.products:
            return self.products[id]
        elif id in self.sales:
            return self.sales[id]
        elif id in self.categories:
            return self.categories[id]
        else:
            raise ValueError(f"ID {id} not found in any table")

    def get_metadata(self, id):
        """
        Получает метаданные по ID
        """
        if id in self.metadata:
            return self.metadata[id]
        else:
            raise ValueError(f"Metadata for ID {id} not found")


# Пример использования
def group_and_sum(tracked_table, group_by_col, sum_col):
    result = {}
    for row in tracked_table:
        group_key = row[group_by_col].value
        if group_key not in result:
            result[group_key] = row[sum_col]
        else:
            result[group_key] += row[sum_col]
    return result


# Пример использования
def test_mock_db():
    # Создаем экземпляр MockDBConnector
    connector = MockDBConnector()

    # Исходные данные (имитация таблицы с ID)
    test_data = [
        {"product": "id_1", "sales": "id_5", "category": "id_9"},
        {"product": "id_2", "sales": "id_6", "category": "id_9"},
        {"product": "id_3", "sales": "id_7", "category": "id_10"},
        {"product": "id_4", "sales": "id_8", "category": "id_11"}
    ]

    # Преобразуем данные с помощью TrackedValue
    tracked_table = []
    for row in test_data:
        tracked_row = {}
        for column, id_value in row.items():
            real_value = connector.get_value(id_value)
            tracked_row[column] = TrackedValue(real_value, [id_value])
        tracked_table.append(tracked_row)

    return tracked_table


# Пример данных
# Создаем отслеживаемую таблицу
tracked_table = test_mock_db()

# Группируем и суммируем
result = group_and_sum(tracked_table, "category", "sales")

# Получаем исходные ID для результата
for group_key, tracked_value in result.items():
    print(f"Group {group_key}: {tracked_value.value}")
    print(f"Source IDs: {tracked_value.source_ids}")