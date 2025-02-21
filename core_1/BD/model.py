from sqlalchemy import Column, Integer, String, MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()
metadata = MetaData()


class DynamicTableManager:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = metadata

    def create_table(self, table_name, columns):
        cols = [Column("_id_", Integer, primary_key=True)]
        cols.extend([Column(col_name, String) for col_name in columns])

        table = Table(table_name, self.metadata, *cols)
        self.metadata.create_all(self.engine)
        return table

    def save_data(self, table, data_list):
        session = self.Session()
        try:
            for row_data in data_list:
                session.execute(table.insert().values(**row_data))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error saving data: {e}")
        finally:
            session.close()

    def get_table_data(self, table):
        session = self.Session()
        try:
            result = session.query(table).all()
            return [{column.name: getattr(row, column.name) for column in table.columns if column.name != 'id'} for row in result]
        finally:
            session.close()


# Пример использования
if __name__ == "__main__":
    db_url = "sqlite:///dynamic_tables.db"
    manager = DynamicTableManager(db_url)

    # Пример данных
    sample_data = [
        {
            "name": {"value": "John", "metadata": {"source": "file1.csv"}},
            "age": {"value": 30, "metadata": {"confidence": 0.9}},
        },
        {
            "name": {"value": "Alice", "metadata": {"source": "file2.csv"}},
            "age": {"value": 25, "metadata": {"confidence": 0.95}},
        },
    ]

    # Создание таблицы
    columns = list(sample_data[0].keys())
    people_table = manager.create_table("people", columns)

    # Сохранение данных
    manager.save_data(people_table, sample_data)

    # Получение данных
    retrieved_data = manager.get_table_data(people_table)
    for row in retrieved_data:
        print(row)
