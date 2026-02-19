import argparse
from app.generate_embeddings import main as generate_embeddings
from app.generate_similar import main as generate_similar
from app.get_similar import main as get_similar, add_args as get_similar_args
from app.generate_feedback import main as generate_feedback
from app.learn_search_model import main as learn_search_model, add_args as learn_search_model_args

def main(args):
    if args.run_embeddings:
        print("Запуск генерации embeddings...")
        generate_embeddings()

    if args.run_feedback:
        print("Запуск генерации feedbacks...")
        generate_feedback()

    if args.run_similar:
        print("Запуск генерации similar...")
        generate_similar()

    if args.run_learn_model:
        print("Запуск обучения поисковой модели...")
        learn_search_model(args)

    if args.run_get_similar:
        print("Запуск получения похожих книг...")
        get_similar(args)

    print("Все выбранные процессы завершены.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Универсальный запуск процессов проекта")
    parser.add_argument("--run-embeddings", action="store_true", help="Генерация embeddings")
    parser.add_argument("--run-feedback", action="store_true", help="Генерация feedbacks")
    parser.add_argument("--run-similar", action="store_true", help="Генерация similar")
    parser.add_argument("--run-learn-model", action="store_true", help="Обучение поисковой модели")
    parser.add_argument("--run-get-similar", action="store_true", help="Получения похожих книг")

    args = parser.parse_args()
    main(args)