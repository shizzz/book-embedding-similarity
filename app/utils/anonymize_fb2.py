import xml.etree.ElementTree as ET
import random
import string
import re

def anonymize_fb2(input_path: str, output_path: str):
    """
    Обезличивает fb2 книгу:
    - заменяет автора на "Test Author"
    - заменяет текст книги на псевдотекст той же структуры
    - сохраняет количество символов, пробелы, пунктуацию и длину слов
    """
    # --- Генератор случайного слова ---
    def random_word(length: int) -> str:
        letters = string.ascii_letters + "абвгдеёжзийклмнопрстуфхцчшщьыъэюя"
        return "".join(random.choice(letters) for _ in range(length))

    # --- Обезличивание текста ---
    def anonymize_text_natural(text: str) -> str:
        if not text:
            return text
        # Разбиваем на слова и не-слова
        tokens = re.findall(r'\w+|\W+', text, flags=re.UNICODE)
        new_tokens = []
        for token in tokens:
            if token.strip() == '':
                # пробелы оставляем
                new_tokens.append(token)
            elif re.match(r'\w+', token, flags=re.UNICODE):
                # слово → заменяем на случайное слово той же длины
                new_tokens.append(random_word(len(token)))
            else:
                # пунктуация оставляем
                new_tokens.append(token)
        return ''.join(new_tokens)

    # --- Замена автора ---
    def anonymize_author(root):
        for author in root.findall(".//author"):
            fn = author.find("first-name")
            ln = author.find("last-name")
            if fn is not None:
                fn.text = "Test"
            if ln is not None:
                ln.text = "Author"

    # --- Рекурсивная обработка всех текстовых узлов ---
    def anonymize_tree(elem):
        if elem.text:
            elem.text = anonymize_text_natural(elem.text)
        for child in elem:
            anonymize_tree(child)
        if elem.tail:
            elem.tail = anonymize_text_natural(elem.tail)

    # --- Основной процесс ---
    tree = ET.parse(input_path)
    root = tree.getroot()

    anonymize_author(root)
    anonymize_tree(root)

    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"Обезличенная книга сохранена в: {output_path}")