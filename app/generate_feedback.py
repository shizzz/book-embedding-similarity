import argparse
from typing import Optional, List
import openai
from app.db import db, BookRepository, SimilarRepository, FeedbackRepository
from app.models import Book
from app.settings.config import DEEPSEEK_API_KEY, OPENAI_API_KEY, LM_STUDIO_BASE_URL

client = None

def get_book_info(book_file: str) -> Optional[Book]:
    with db() as conn:
        book_row = BookRepository().get_by_file(conn, book_file)
        if not book_row:
            return None
        return Book.map(book_row)

def get_similar_books(source_book_id: int) -> List[tuple]:
    with db() as conn:
        similars = SimilarRepository().get(conn, source_book_id, limit=100)
        return similars

def generate_feedback_prompt(source_book: Book, candidate_book: Book) -> str:
    prompt = f"""
    Оцени похожесть между двумя книгами от 0 до 100.
    0: совсем не похожи, не понравится читателю
    100: очень похожи, понравится читателю
    
    Исходная книга:
    Название: {source_book.title}
    Автор: {source_book.author}
    
    Кандидат:
    Название: {candidate_book.title}
    Автор: {candidate_book.author}
    
    Ответь только целым числом от 0 до 100 без пояснений.
    """
    return prompt

def call_chatgpt(prompt: str) -> int | None:
    client = openai.OpenAI(
        api_key=OPENAI_API_KEY,
        base_url="https://api.chatanywhere.tech/v1"
    )
    
    try:  
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1
        )
        
        # Извлекаем число из ответа
        content = response.choices[0].message.content.strip()
        return int(content)
    except Exception as e:
        print(f"Ошибка вызова API: {e}")
        return None

def call_deepseek(prompt: str) -> int | None:
    """Вызвать DeepSeek API и получить оценку"""
    try:
        api_key = DEEPSEEK_API_KEY
        # Используем DeepSeek API endpoint
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1"
        )
        
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1
        )
        
        # Извлекаем число из ответа
        content = response.choices[0].message.content.strip()
        return int(content)
    except Exception as e:
        print(f"Ошибка вызова API: {e}")
        return None
    
def call_lm_studio(prompt: str) -> int | None:
    """Вызвать LM Studio API и получить оценку"""
    try:
        # Используем LM Studio API endpoint
        client = openai.OpenAI(
            api_key="lm-studio",  # LM Studio использует любой ключ
            base_url=LM_STUDIO_BASE_URL,  # По умолчанию локальный запуск
        )
        
        response = client.chat.completions.create(
            model="qwen/qwen3-coder-30b",  # Имя модели в LM Studio
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=10
        )
        
        # Извлекаем число из ответа
        content = response.choices[0].message.content.strip()
        return int(content)
    except Exception as e:
        print(f"Ошибка вызова API: {e}")
        return None

def save_feedback(source_book_id: int, candidate_book_id: int, label: float):
    with db() as conn:
        FeedbackRepository().submit(conn, source_book_id, candidate_book_id, label)

def main(book_file: str):
    source_book = get_book_info(book_file)
    if not source_book:
        print(f"Книга {book_file} не найдена")
        return
    
    print(f"Исходная книга: {source_book.title} - {source_book.author}")
    
    similars = get_similar_books(source_book.id)
    if not similars:
        print("Похожих книг не найдено")
        return
    
    print(f"Найдено {len(similars)} похожих книг")
    
    for _, _, candidate_id in similars:
        candidate_book = get_book_info_by_id(candidate_id)
        if not candidate_book:
            continue
        
        print(f"Обработка: {source_book.title} - {source_book.author} <- {candidate_book.title} - {candidate_book.author}")
        
        prompt = generate_feedback_prompt(source_book, candidate_book)
        
        try:
            label = call_chatgpt(prompt)
            print(f"  Оценка: {label}")
            
            if label != None:
                devided = label / 100
                save_feedback(source_book.id, candidate_id, devided)
            
        except Exception as e:
            print(f"  Ошибка обработки: {e}")

def get_book_info_by_id(book_id: int) -> Optional[Book]:
    with db() as conn:
        book_row = BookRepository().get_by_id(conn, book_id)
        if not book_row:
            return None
        return Book.map(book_row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Генерация feedback для книг с помощью ChatGPT")
    parser.add_argument("book", help="Имя файла книги (например: 1234.fb2)")
    
    args = parser.parse_args()
    
    main(args.book)
