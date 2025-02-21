from core_2 import MockDBConnector


def create_tracked_table(data_with_ids, db_connector):
    """
    Создает две таблицы: значения и source_ids
    """
    values_table = []  # Список словарей с реальными значениями
    sources_table = []  # Список словарей с source_ids

    for row in data_with_ids:
        values_row = {}
        sources_row = {}
        for col, id_value in row.items():
            values_row[col] = db_connector.get_value(id_value)
            sources_row[col] = {id_value}  # Используем множество для source_ids
        values_table.append(values_row)
        sources_table.append(sources_row)

    return values_table, sources_table


def group_and_sum(values_table, sources_table, group_by_col, sum_col):
    """
    Группирует данные и отслеживает source_ids
    """
    result_values = {}
    result_sources = {}

    for i in range(len(values_table)):
        group_key = values_table[i][group_by_col]
        value = values_table[i][sum_col]
        sources = sources_table[i][sum_col]

        if group_key not in result_values:
            result_values[group_key] = value
            result_sources[group_key] = sources
        else:
            result_values[group_key] += value
            result_sources[group_key].update(sources)

    return result_values, result_sources


# Пример использования:
def test_mock_db():
    connector = MockDBConnector()

    test_data = [
        {"product": "id_1", "sales": "id_5", "category": "id_9"},
        {"product": "id_2", "sales": "id_6", "category": "id_9"},
        {"product": "id_3", "sales": "id_7", "category": "id_10"},
        {"product": "id_4", "sales": "id_8", "category": "id_11"},
    ]

    # Получаем две таблицы
    values_table, sources_table = create_tracked_table(test_data, connector)

    # Группируем и суммируем
    result_values, result_sources = group_and_sum(
        values_table, sources_table, "category", "sales"
    )

    # Выводим результаты
    for group_key in result_values:
        print(f"\nGroup {group_key}:")
        print(f"Value: {result_values[group_key]}")
        print(f"Source IDs: {result_sources[group_key]}")

        # Если нужны метаданные
        for source_id in result_sources[group_key]:
            metadata = connector.get_metadata(source_id)
            print(f"  From: {metadata['filename']}, row {metadata['row']}")


# Выполняем тест
test_mock_db()
