import re
from collections import Counter

def tokenize(text: str):
    """
    Split text into words.
    - Convert to lowercase
    - Remove punctuation
    - Remove extra whitespace
    Returns: List of words
    """
    tokens = re.split(r'[\W\s]+', text.lower())

    # remove later?
    t = tokens[len(tokens)-1]
    if t == "":
        tokens.remove(t)
    return tokens

import re

COMMON_ABBREVIATIONS = {
    "Mr.", "Mrs.", "Ms.", "Dr.", "Prof.",
    "Sr.", "Jr.", "St.", "vs.", "etc.",
    "e.g.", "i.e.", "a.m.", "p.m.", "U.S.",
    "U.S.A.", "Inc.", "Ltd.", "Co.", "Jan."
}

def get_sentences(text: str):
    """
    Split text into sentences.
    - Handle abbreviations (Dr., Mr., etc.)
    - Handle multiple punctuation (!! or ...)
    Returns: List of sentences
    """

    placeholder = "<DOT>"
    protected = text

    # protect abbreviations
    for abbr in COMMON_ABBREVIATIONS:
        protected = protected.replace(abbr, abbr.replace(".", placeholder))

    tokens = re.split(r'(?<=[.!?])\s+', protected.lower())

    # restore periods
    tokens = [t.replace(placeholder, ".") for t in tokens]

    return tokens

def get_ngrams(words, n):
    """
    Generate n-grams from a list of words.
    Example: get_ngrams(['a', 'b', 'c'], 2) -> [('a', 'b'), ('b', 'c')]
    Returns: List of tuples
    """
    ngrams = []
    for i in range(len(words) - n + 1):
        word = words[i]
        ngrams.append(tuple(words[i:i+n]))
    return ngrams

def remove_stopwords(words: list, stopwords=None):
    """
    Remove common stopwords from word list.
    Use a default set if stopwords not provided.
    Returns: Filtered list of words
    """
    stopwords = [
    "a", "as", "an", "the", "and", "or", "but",
    "if", "while", "of", "at", "by", "for", "with", "about",
    "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
    "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "i", "you", "he", "she", "it", "we", "they",
    "me", "him", "her", "them", "my", "your", "his", "their", "our",
    "this", "that", "these", "those", "am", "can", "could", "should", "would", "will", "just"
    ]

    for stopword in stopwords:
        if stopword in words:
            words.remove(stopword)
    return words
