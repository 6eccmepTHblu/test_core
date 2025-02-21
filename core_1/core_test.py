import xlsxwriter
import zipfile
import io
import msgpack
import gzip
import lz4.frame

from lxml import etree
from core_1.BD.model import DynamicTableManager
from core_1.dop_function import execution_time
from get_dat.CalamineLoaderExcel import CalamineLoaderExcel


@execution_time
def main():
    # Заготовленные параметры
    path_file = r"D:\Данные\AK SS\Данные 16\Инспекции на P2 06.02.25.xlsx"
    sheet_number = 1
    db_url = "sqlite:///dynamic_tables.db"
    file_save_name_xlsx = "output.xlsx"
    file_save_name_xmlz = "output.xmlz"
    file_save_name_msgpack = "output.msgpack"
    file_save_name_msgpackZ = "output_zip.msgpack"
    file_save_name_lz4 = "output_lz4.msgpack"

    # Получаем данные из файла
    common_data = get_data_from_excel(path_file, sheet_number)

    # Конвертируем данные в словарь
    dict_data = converter_data_in_dict(common_data)

    # === БД ===========================================================================
    # Заполняем БД данными
    print('=== БД ====================')
    filling_DB_with_data(db_url, "people", dict_data)

    # === Excel ===========================================================================
    print('=== Excel ====================')
    # Сохраняем данные в файл
    save_data_to_excel_fast(dict_data, file_save_name_xlsx)

    # === xml ===========================================================================
    print('=== xml ====================')
    # Сохраняем данные в xml
    save_data_to_compressed_xml(dict_data, file_save_name_xmlz)

    # Получение данные в xml
    read_compressed_xml(file_save_name_xmlz)

    # === msgpack ===========================================================================
    print('=== msgpack ====================')
    # Сохраняем данные в msgpack
    save_data_fast(dict_data, file_save_name_msgpack)

    # Получение данные в msgpack
    data_1 = read_data_fast(file_save_name_msgpack)

    # === msgpack zip ===========================================================================
    print('=== msgpack zip ====================')
    # Сохраняем данные в msgpack
    save_compressed_msgpack(dict_data, file_save_name_msgpackZ)

    # Получение данные в msgpack
    data_2 = load_compressed_msgpack(file_save_name_msgpackZ)

    # === msgpack lz4 ===========================================================================
    print('=== msgpack lz4 ====================')
    # Сохраняем данные в msgpack
    save_lz4_msgpack(dict_data, file_save_name_lz4)

    # Получение данные в msgpack
    data_3 = load_lz4_msgpack(file_save_name_lz4)

    pass


@execution_time
def get_data_from_excel(path_file: str, sheet_number: int) -> list[list[str]]:
    """Получаем данные из указанного файла"""
    cal_manager = CalamineLoaderExcel(path_file)

    # Проверяем указанный лист
    count_sheets = cal_manager.get_sheet_names()
    if sheet_number >= len(count_sheets):
        raise ValueError(f"Лист с номером {sheet_number} не найден")

    # Получаем указанный лист
    sheet = cal_manager.get_sheet_by_index(sheet_number)

    # Получаем данные из листа
    data = cal_manager.get_data(sheet)
    return data


@execution_time
def converter_data_in_dict(data: list[list[str]]) -> list[dict[str, str]]:
    """Конвертируем данные в словарь"""
    headers = data[0]
    headers = [header.split(" / ")[0] for header in headers]
    data = data[1:]
    result = []
    for row in data:
        result.append(dict(zip(headers, row)))
    return result


@execution_time
def filling_DB_with_data(
    db_url: str, table_name: str, data: list[dict[str, str]]
) -> None:
    """Заполняем БД данными"""
    # Подключаем БД
    manager = DynamicTableManager(db_url)

    # Создание таблицы
    columns = list(data[0].keys())
    people_table = manager.create_table(table_name, columns)

    # Сохранение данных
    manager.save_data(people_table, data)


@execution_time
def save_data_to_excel_fast(data, filename):
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()

    # Записываем заголовки
    headers = list(data[0].keys())
    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    # Записываем данные
    for row, record in enumerate(data, start=1):
        for col, value in enumerate(record.values()):
            worksheet.write(row, col, value)

    workbook.close()


@execution_time
def save_data_to_compressed_xml(data, filename):
    root = etree.Element("data")

    for row in data:
        row_elem = etree.SubElement(root, "row")
        for key, value in row.items():
            cell = etree.SubElement(row_elem, "cell")
            cell.set("name", key)
            cell.text = str(value)

    tree = etree.ElementTree(root)

    # Создаем сжатый файл
    with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as zf:
        # Записываем XML в память
        xml_data = io.BytesIO()
        tree.write(xml_data, xml_declaration=True, encoding="utf-8", pretty_print=True)
        xml_data.seek(0)

        # Добавляем XML в архив
        zf.writestr("data.xml", xml_data.getvalue())


@execution_time
def read_compressed_xml(filename):
    with zipfile.ZipFile(filename, "r") as zf:
        with zf.open("data.xml") as f:
            tree = etree.parse(f)
            root = tree.getroot()

            data = []
            for row_elem in root.findall("row"):
                row_data = {}
                for cell in row_elem.findall("cell"):
                    row_data[cell.get("name")] = cell.text
                data.append(row_data)

            return data


@execution_time
def save_data_fast(data, filename):
    with open(filename, "wb") as f:
        msgpack.pack(data, f)


@execution_time
def read_data_fast(filename):
    with open(filename, "rb") as f:
        return msgpack.unpack(f)


@execution_time
def save_compressed_msgpack(data, filename):
    packed = msgpack.packb(data)
    with gzip.open(filename, "wb") as f:
        f.write(packed)


@execution_time
def load_compressed_msgpack(filename):
    with gzip.open(filename, "rb") as f:
        packed = f.read()
    return msgpack.unpackb(packed)

@execution_time
def save_lz4_msgpack(data, filename):
    packed = msgpack.packb(data)
    compressed = lz4.frame.compress(packed)
    with open(filename, 'wb') as f:
        f.write(compressed)

@execution_time
def load_lz4_msgpack(filename):
    with open(filename, 'rb') as f:
        compressed = f.read()
    packed = lz4.frame.decompress(compressed)
    return msgpack.unpackb(packed)


if __name__ == "__main__":
    main()
