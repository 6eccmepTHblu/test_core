import fnmatch

from python_calamine import CalamineWorkbook, CalamineSheet
from typing import List, Union, Optional


class CalamineLoaderExcel:
    """
    Класс для работы с Excel файлами через библиотеку python-calamine.
    Предоставляет удобные методы для получения листов и данных.
    """

    def __init__(self, file_path: str):
        """
        Инициализация loader'а с путём к Excel файлу.

        Args:
            file_path (str): Путь к Excel файлу
        """
        self.file_path = file_path
        self.workbook = CalamineWorkbook.from_path(file_path)

    def get_sheet_names(self) -> List[str]:
        """
        Получить список имен всех листов в книге.

        Returns:
            List[str]: Список имен листов
        """
        return self.workbook.sheet_names

    def get_sheet_by_index(self, index: int) -> Optional[CalamineSheet]:
        """
        Получить лист по индексу.

        Args:
            index (int): Индекс листа (начиная с 0)

        Returns:
            Optional[Worksheet]: Лист или None, если индекс невалиден
        """
        sheets = self.get_sheet_names()
        if 0 <= index < len(sheets):
            return self.workbook.get_sheet_by_index(index)
        return None

    def get_sheets_by_patterns(self, patterns: List[str]) -> List[CalamineSheet]:
        """
        Получить листы, соответствующие заданным маскам.

        Args:
            patterns (List[str]): Список масок (например: ["Sheet*", "Data*"])

        Returns:
            List[Worksheet]: Список найденных листов
        """
        sheets = self.get_sheet_names()
        matching_sheets = []

        for sheet_name in sheets:
            for pattern in patterns:
                if fnmatch.fnmatch(sheet_name, pattern):
                    worksheet = self.workbook.get_sheet_by_name(sheet_name)
                    if worksheet:
                        matching_sheets.append(worksheet)
                    break

        return matching_sheets

    def get_sheet_data(
        self, sheet: Union[CalamineSheet, str, int]
    ) -> Optional[List[List]]:
        """
        Получить данные с указанного листа.

        Args:
            sheet: Лист (можно передать Worksheet, имя листа или его индекс)

        Returns:
            Optional[List[List]]: Данные листа в виде списка списков или None при ошибке
        """
        worksheet = None

        if isinstance(sheet, CalamineSheet):
            worksheet = sheet
        elif isinstance(sheet, str):
            worksheet = self.workbook.get_sheet_by_name(sheet)
        elif isinstance(sheet, int):
            worksheet = self.get_sheet_by_index(sheet)

        if worksheet:
            return worksheet.to_python(skip_empty_area=False)
        return None

    def get_sheet_by_name(self, name: str) -> Optional[CalamineSheet]:
        """
        Получить лист по его имени.

        Args:
            name (str): Имя листа

        Returns:
            Optional[Worksheet]: Лист или None, если имя невалидно
        """
        return self.workbook.get_sheet_by_name(name)

    def __del__(self):
        """
        Закрываем workbook при уничтожении объекта
        """
        if hasattr(self, 'workbook'):
            self.workbook.close()

    @staticmethod
    def get_data(sheet: CalamineSheet):
        result = sheet.to_python(skip_empty_area=False)

        # Преобразование данных в строки

        for r, row in enumerate(result):
            for c, cell in enumerate(row):
                result[r][c] = str(cell)

        return result
