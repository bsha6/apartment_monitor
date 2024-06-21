import re


def extract_digits_from_text(text: str) -> int:
    """Given a string, extract all digits using regex."""
    digits = re.sub(r'\D', '', text)
    return digits


def snakecase_text(text: str) -> str:
    """Snakecase by lowercasing all text, replacing spaces with '_', and replacing unneeded characters."""
    lowercased_text = text.lower()
    replaced_text = lowercased_text.replace("*", "")
    snakecased_text = re.sub(r'\s+', '_', replaced_text.strip())
    return snakecased_text
