from typing import Optional, List
import openai
from app.db import DBRouter
from app.db.repositories import BookRepository, SimilarRepository, FeedbackRepository
from app.models import Book
from app.settings.config import DEEPSEEK_API_KEY, OPENAI_API_KEY, LM_STUDIO_BASE_URL

router = DBRouter()

def run(args):
    book_file = args.book
    ai_api = args.ai

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
            label = ai_callers[ai_api](prompt)
                
            print(f"  Оценка: {label}")
            
            if label not in (None, 0):
                score = label if label == -1 else label / 100
                save_feedback(source_book.id, candidate_id, score)
            
        except Exception as e:
            print(f"  Ошибка обработки: {e}")


def get_book_info(book_file: str) -> Optional[Book]:
    book_row = BookRepository(router).get_by_file(book_file)
    if not book_row:
        return None
    return Book.from_row(book_row)

def get_similar_books(source_book_id: int) -> List[tuple]:
    similars = SimilarRepository(router).get(source_book_id, limit=100)
    return similars

def get_book_info_by_id(book_id: int) -> Optional[Book]:
    return BookRepository(router).get_by_id(book_id)

def save_feedback(source_book_id: int, candidate_book_id: int, label: float):
    FeedbackRepository(router).submit(source_book_id, candidate_book_id, label)

def generate_feedback_prompt(source_book: Book, candidate_book: Book) -> str:
    prompt = f"""
Оцени схожесть двух книг:
-1 — совсем не похожи
0 — без оценки
1–100 — очень похожи, понравится читателю

Исходная книга:
Название: {source_book.title}
Автор: {source_book.author}

Кандидат:
Название: {candidate_book.title}
Автор: {candidate_book.author}

Ответь ТОЛЬКО целым числом (-1, 0–100), без пояснений.
Примеры допустимых ответов:
-1
0
75
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
            max_tokens=3
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
            max_tokens=3
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

ai_callers = {
    "chatgpt": call_chatgpt,
    "deepseek": call_deepseek,
    "lm_studio": call_lm_studio
}