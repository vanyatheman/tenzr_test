import json
import sqlite3

DB = "offices.db"
JSON_DATA = "data.json"


class DataImproter:
    """Импорт JSON данных в БД."""

    def __init__(self, json_file, db):
        self.json_file = json_file
        self.db = db

    def import_data(self):
        # Подгружаем JSON файл.
        with open(self.json_file, "r", encoding="utf8") as f:
            data = f.read()
        json_data = json.loads(data)

        # Создает БД SQLite.
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()

        # Создает таблицу данных data.
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER PRIMARY KEY,
                ParentId INTEGER,
                Name TEXT,
                Type INTEGER
            )
        """
        )

        # Вставляем JSON данные в табилцу data.
        for line in json_data:
            cursor.execute(
                """
                 INSERT OR IGNORE INTO data (id, ParentId, Name, Type)
                VALUES (?, ?, ?, ?)
            """,
                (line["id"], line["ParentId"], line["Name"], line["Type"]),
            )

        # Отправляем запросы.
        conn.commit()

        # Закрываем соединение с БД.
        conn.close()


class StaffFetcher:
    """Получение списка сотрудников в офисе."""

    def __init__(self, db):
        self.db = db

    def get_staff(self, person_id):
        # Подключаемся к БД.
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()

        # Находим офис сотрудника по его id.
        cursor.execute(
            """
            WITH RECURSIVE Rec AS (
                SELECT id, ParentId, Name, Type, 0 AS level
                FROM data
                WHERE id = ?
                
                UNION ALL
                
                SELECT d.id, d.ParentId, d.Name, d.Type, Rec.level + 1
                FROM data d 
                INNER JOIN Rec ON d.id = Rec.ParentId
            )
            SELECT Name 
            FROM Rec
            WHERE Type = 1
            LIMIT 1
        """,
            (person_id,),
        )
        office = cursor.fetchone()[0]

        # Получаем всех сотрудников этого офиса .
        cursor.execute(
            """
            WITH RECURSIVE Rec AS (
                SELECT id, ParentId, Name, Type, 0 AS level
                FROM data 
                WHERE ParentId = (
                    SELECT id FROM data
                    WHERE Name = ? AND Type = 1
                )
                
                UNION ALL
                
                SELECT d.id, d.ParentId, d.Name, d.Type, Rec.level + 1
                FROM data d
                INNER JOIN Rec ON d.ParentId = Rec.id
            )
            SELECT Name 
            FROM Rec
            WHERE Type = 3
            ORDER BY Name
        """,
            (office,),
        )
        staff = [row[0] for row in cursor.fetchall()]

        conn.close()
        return staff


def main():
    person_id = int(input("Введите id сотрудника: "))

    importer = DataImproter(JSON_DATA, DB)
    importer.import_data()

    fetcher = StaffFetcher(DB)
    staff = fetcher.get_staff(person_id)

    print("Все сотрудники офиса: ", *staff)


if __name__ == "__main__":
    main()
