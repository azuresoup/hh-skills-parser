import requests
import sqlite3
import json
import time
import sys
from typing import List, Dict, Optional

# Константы для поиска и фильтрации
class SearchConstants:
    # Поисковые термины (можно заменить на python, java, javascript и т.д.)
    DEFAULT_SEARCH_QUERY = "golang OR go developer"
    LANGUAGE_KEYWORDS = ['go', 'golang']
    
    # Исключаемые слова (тут руководящие позиции, но можно вставить свои слова)
    EXCLUDED_WORDS = ['lead', 'лид', 'руководитель', 'ментор', 'преподаватель', 'менеджер']

class HHVacancyParser:
    def __init__(self, db_path: str = "vacancies.db"):
        self.db_path = db_path
        self.base_url = "https://api.hh.ru"
        self.headers = {
            'User-Agent': 'vacancy-parser-python'
        }
        self.init_database()
    
    def init_database(self):
        """Создает таблицу для вакансий если её нет"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                hh_id VARCHAR(50) UNIQUE,
                title VARCHAR(500),
                description TEXT,
                skills TEXT,
                url VARCHAR(500),
                employer VARCHAR(300),
                salary_from INTEGER,
                salary_to INTEGER,
                currency VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        print("База данных инициализирована")
    
    def vacancy_exists(self, hh_id: str) -> bool:
        """Проверяет существует ли вакансия в БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM vacancies WHERE hh_id = ?", (hh_id,))
        exists = cursor.fetchone() is not None
        
        conn.close()
        return exists
    
    def save_vacancy(self, vacancy_data: Dict):
        """Сохраняет вакансию в БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO vacancies 
                (hh_id, title, description, skills, url, employer, salary_from, salary_to, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vacancy_data['hh_id'],
                vacancy_data['title'],
                vacancy_data['description'],
                vacancy_data['skills'],
                vacancy_data['url'],
                vacancy_data['employer'],
                vacancy_data['salary_from'],
                vacancy_data['salary_to'],
                vacancy_data['currency']
            ))
            
            conn.commit()
            print(f"✓ Добавлена вакансия: {vacancy_data['title']}")
            
        except sqlite3.IntegrityError:
            print(f"⚠ Вакансия {vacancy_data['hh_id']} уже существует")
        except Exception as e:
            print(f"✗ Ошибка при сохранении: {e}")
        finally:
            conn.close()
    
    def search_vacancies(self, text: str = SearchConstants.DEFAULT_SEARCH_QUERY, area: str = None) -> List[Dict]:
        """Ищет ВСЕ вакансии по запросу"""
        all_vacancies = []
        page = 0
        
        while True:
            print(f"Загружаю страницу {page + 1}...")
            
            params = {
                'text': text,
                'page': page,
                'per_page': 100,
                'search_field': 'name'  # Поиск только в названии вакансии
            }
            
            # Добавляем area только если указан
            if area:
                params['area'] = area
            
            try:
                response = requests.get(
                    f"{self.base_url}/vacancies",
                    headers=self.headers,
                    params=params,
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    vacancies = data.get('items', [])
                    total_pages = data.get('pages', 0)
                    total_found = data.get('found', 0)
                    
                    if page == 0:
                        print(f"Всего найдено: {total_found} вакансий на {total_pages} страницах")
                    
                    # Дополнительная фильтрация по названию
                    filtered_vacancies = []
                    for vacancy in vacancies:
                        title = vacancy['name'].lower()
                        # Ищем ключевые слова в названии
                        if any(keyword in title for keyword in SearchConstants.LANGUAGE_KEYWORDS):
                            # Исключаем слова из EXCLUDED_WORDS
                            if not any(word in title for word in SearchConstants.EXCLUDED_WORDS):
                                filtered_vacancies.append(vacancy)
                            else:
                                print(f"  ✗ {vacancy['name']} (Исключаемые слова)")
                        else:
                            print(f"  ✗ {vacancy['name']} (Нет ключевых слов)")
                    
                    all_vacancies.extend(filtered_vacancies)
                    
                    print(f"Страница {page + 1}: найдено {len(vacancies)}, отфильтровано: {len(filtered_vacancies)}")
                    
                    # Если это последняя страница - выходим
                    if len(vacancies) == 0 or page >= total_pages - 1:
                        print("Все страницы загружены!")
                        break
                        
                else:
                    print(f"Ошибка API: {response.status_code}")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"Ошибка запроса: {e}")
                break
            
            page += 1
            
            # Увеличенная пауза для безопасности
            time.sleep(3)
        
        return all_vacancies
    
    def get_vacancy_details(self, vacancy_id: str) -> Optional[Dict]:
        """Получает детальную информацию о вакансии"""
        try:
            response = requests.get(
                f"{self.base_url}/vacancies/{vacancy_id}",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка получения деталей вакансии {vacancy_id}: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса деталей: {e}")
            return None
    
    def parse_vacancy_data(self, vacancy_summary: Dict, vacancy_details: Dict) -> Dict:
        """Парсит данные вакансии"""
        # Извлекаем навыки
        skills = []
        if vacancy_details and 'key_skills' in vacancy_details:
            skills = [skill['name'] for skill in vacancy_details['key_skills']]
        
        # Описание
        description = ""
        if vacancy_details and 'description' in vacancy_details:
            # Убираем HTML теги из описания
            import re
            description = re.sub('<[^<]+?>', '', vacancy_details['description'])
            description = description.strip()
        
        # Зарплата
        salary_from = None
        salary_to = None
        currency = None
        
        if vacancy_summary.get('salary'):
            salary = vacancy_summary['salary']
            salary_from = salary.get('from')
            salary_to = salary.get('to')
            currency = salary.get('currency')
        
        return {
            'hh_id': vacancy_summary['id'],
            'title': vacancy_summary['name'],
            'description': description[:10000],  # Ограничиваем длину
            'skills': json.dumps(skills, ensure_ascii=False),
            'url': vacancy_summary['alternate_url'],
            'employer': vacancy_summary['employer']['name'],
            'salary_from': salary_from,
            'salary_to': salary_to,
            'currency': currency
        }
    
    def run(self):
        """Основной метод запуска парсера"""
        print("🚀 Запуск парсера вакансий HeadHunter")
        print("=" * 50)
        
        # Поиск вакансий
        vacancies = self.search_vacancies()
        print(f"\n📊 Всего найдено: {len(vacancies)} вакансий")
        
        new_count = 0
        existing_count = 0
        
        for i, vacancy in enumerate(vacancies, 1):
            vacancy_id = vacancy['id']
            
            # Проверяем существование в БД
            if self.vacancy_exists(vacancy_id):
                existing_count += 1
                print(f"[{i}/{len(vacancies)}] ⏭ Пропускаю (уже есть): {vacancy['name']}")
                continue
            
            # Получаем детали вакансии
            print(f"[{i}/{len(vacancies)}] 📥 Загружаю детали: {vacancy['name']}")
            details = self.get_vacancy_details(vacancy_id)
            
            # Парсим и сохраняем
            vacancy_data = self.parse_vacancy_data(vacancy, details)
            self.save_vacancy(vacancy_data)
            
            new_count += 1
            
            # Пауза между запросами (УВЕЛИЧЕНА для безопасности)
            time.sleep(2)
        
        print("\n" + "=" * 50)
        print("✅ Парсинг завершен!")
        print(f"📈 Новых вакансий: {new_count}")
        print(f"📋 Уже существовало: {existing_count}")

def main():
    parser = HHVacancyParser()
    parser.run()

if __name__ == "__main__":
    main()