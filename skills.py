import sqlite3
import json
import re
import bleach
from collections import Counter

# Константы
TOP_SKILLS_LIMIT = 50
TOP_KEYWORDS_LIMIT = 50

# Стоп-слова для фильтрации
STOP_WORDS = {
    # Числа
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '00', '10', '11', '12', '13', '14', '15', 
    '16', '17', '18', '19', '20', '30', '39', '50', '60', '70', '80', '90', '100', '000',
    
    # Английские служебные слова
    'and', 'the', 'to', 'of', 'you', 'in', 'with', 'for', 'a', 'an', 'is', 'are', 'be', 
    'been', 'have', 'has', 'had', 'will', 'would', 'could', 'should', 'may', 'might',
    'this', 'that', 'these', 'those', 'he', 'she', 'it', 'they', 'we', 'us', 'our',
    
    # Общие IT слова
    'it', 'skills', 'back', 'end', 'experience', 'work', 'working', 'job', 'position', 
    'role', 'team', 'project', 'projects', 'development', 'developer', 'specialist',
    'engineer', 'technology', 'technologies', 'months', 'years', 'code', 'review',
    'senior', 'junior', 'middle', 'lead', 'data', 'science', 'web', 'your', 'our',
    'will', 'can', 'must', 'need', 'good', 'strong', 'excellent', 'high', 'low',
    'design', 'support', 'on', 'node', 'js',
    
    # Названия компаний (не технологии)
    'ozon', 'yandex', 'google', 'microsoft', 'apple', 'amazon', 'facebook', 'meta',
    'sber', 'tinkoff', 'avito', 'wildberries', 'kaspersky', 'jetbrains',
    
    # График работы и прочий мусор
    '5', '2', 'b2b', 'b2c', 'java', 'schedule', 'remote', 'office', 'salary',
    
    # Технический мусор  
    'quot', 'ru', 'etc', 'er', 'e', 'o', 'nbsp', 'amp', 'gt', 'lt', 'strong', 'em', 'br',
    'div', 'span', 'p', 'ul', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
}

class SkillAnalyzer:
    def __init__(self, db_path: str = "vacancies.db"):
        self.db_path = db_path
    
    def analyze_skills(self):
        """Анализирует частоту навыков из key_skills"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT skills FROM vacancies WHERE skills IS NOT NULL AND skills != ''")
        results = cursor.fetchall()
        conn.close()
        
        # Собираем все навыки
        all_skills = []
        for (skills_json,) in results:
            try:
                skills = json.loads(skills_json)
                all_skills.extend(skills)
            except:
                continue
        
        # Подсчитываем
        counter = Counter(all_skills)
        
        print(f"🔧 НАВЫКИ ИЗ KEY_SKILLS (топ {TOP_SKILLS_LIMIT}):")
        print(f"Всего упоминаний навыков: {len(all_skills)}")
        print(f"Уникальных навыков: {len(counter)}")
        print()
        
        # Выводим топ с нумерацией
        for i, (skill, count) in enumerate(counter.most_common(TOP_SKILLS_LIMIT), 1):
            print(f"{i}. {skill}: {count}")
    
    def extract_keywords_from_text(self, text):
        """Извлекает ключевые слова из текста описания"""
        if not text:
            return []
        
        # Очищаем HTML теги
        clean_text = bleach.clean(text, tags=[], strip=True)
        
        # Извлекаем слова и составные термины (с дефисами, слешами)
        words = re.findall(r'[A-Za-z0-9]+(?:[/\-][A-Za-z0-9]+)*', clean_text)
        
        # Приводим к нижнему регистру и фильтруем стоп-слова
        keywords = []
        for word in words:
            word = word.lower()
            if len(word) >= 2 and word not in STOP_WORDS:
                keywords.append(word)
        
        return keywords
    
    def analyze_keywords(self):
        """Анализирует ключевые слова из описаний вакансий"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT description FROM vacancies WHERE description IS NOT NULL AND description != ''")
        results = cursor.fetchall()
        conn.close()
        
        # Собираем все ключевые слова
        all_keywords = []
        print("Обрабатываю описания вакансий...")
        
        for i, (description,) in enumerate(results, 1):
            if i % 50 == 0:  # Показываем прогресс каждые 50 вакансий
                print(f"Обработано: {i}/{len(results)}")
            
            keywords = self.extract_keywords_from_text(description)
            all_keywords.extend(keywords)
        
        # Подсчитываем
        counter = Counter(all_keywords)
        
        print(f"\n📝 КЛЮЧЕВЫЕ СЛОВА ИЗ ОПИСАНИЙ (топ {TOP_KEYWORDS_LIMIT}):")
        print(f"Всего слов найдено: {len(all_keywords)}")
        print(f"Уникальных слов: {len(counter)}")
        print()
        
        # Выводим топ с нумерацией
        for i, (keyword, count) in enumerate(counter.most_common(TOP_KEYWORDS_LIMIT), 1):
            print(f"{i}. {keyword}: {count}")
    
    def run_full_analysis(self):
        """Запускает полный анализ навыков и ключевых слов"""
        print("АНАЛИЗ ВАКАНСИЙ")
        print("=" * 60)
        
        # Основная статистика
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vacancies")
        total_vacancies = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM vacancies WHERE skills IS NOT NULL AND skills != ''")
        vacancies_with_skills = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM vacancies WHERE description IS NOT NULL AND description != ''")
        vacancies_with_description = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"Всего вакансий в базе: {total_vacancies}")
        print(f"Вакансий с навыками: {vacancies_with_skills}")
        print(f"Вакансий с описанием: {vacancies_with_description}")
        print()
        
        # Анализ навыков
        self.analyze_skills()
        
        print("\n" + "=" * 60)
        
        # Анализ ключевых слов
        self.analyze_keywords()

def main():
    analyzer = SkillAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main()