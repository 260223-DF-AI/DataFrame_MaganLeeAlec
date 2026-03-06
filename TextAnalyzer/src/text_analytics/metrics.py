import re

def flesch_reading_ease(word_count, sentence_count, syllable_count):
    """
    Calculate Flesch Reading Ease score.
    Formula: 206.835 - 1.015 * (words/sentences) - 84.6 * (syllables/words)
    
    Score interpretation:
    - 90-100: Very easy (5th grade)
    - 60-70: Standard (8th-9th grade)
    - 30-50: Difficult (college)
    - 0-30: Very difficult (college graduate)
    """
    if sentence_count <= 0 or word_count <= 0:
        return 0.0
    
    words_per_sentence = word_count / sentence_count
    syllables_per_word = syllable_count / word_count
    #the flesch reading each formula
    #206.835 = baseline readaility score, 1.015 = penalizes long sentences, 84.6 = penalizes complex words
    score = 206.835 - 1.015 * words_per_sentence - 84.6 * syllables_per_word
    return float(score)

def count_syllables(word):
    """
    Count syllables in a word.
    Simple heuristic: count vowel groups.
    """
    #return 0 if the word is empty or none
    if not word:
        return 0
    #normalize the word.  lowercase and remove non-letter characters
    w = word.lower()
    w = re.sub(r"[^a-z]", "", w)
    if not w:
        return 0
    #accounts for vowel letters
    vowels = "aeiouy"
    count = 0
    prev_is_vowel = False
    #count groups of consecutive vowels as one syllable
    for ch in w:
        is_vowel = ch in vowels
        if is_vowel and not prev_is_vowel:
            count += 1
        prev_is_vowel = is_vowel
    #adjust the silent "e" at the end of some words
    if w.endswith("e") and count > 1 and w[-2] not in vowels:
        count -= 1
    #ensure every word has at least one syllable
    return max(1,count) if len(w) > 2 else max(1,count)

def calculate_readability(analyzer):
    """
    Calculate readability metrics for an analyzed document.
    Returns: Dict with various readability scores
    """
    #get total number of words and sentences from the analyher object
    #getattr is used in case the attributes dont exist
    word_count = len(getattr(analyzer, "words", []) or [])
    sentence_count = len(getattr(analyzer, "sentences", []) or [])
    #count total syllables in the document by iterating over each word and counting its syllables
    syllable_count = 0
    for w in getattr(analyzer, "words", []) or []:
        syllable_count += count_syllables(w)
    #calculate the flesch reading ease score
    fre = flesch_reading_ease(word_count, sentence_count, syllable_count)
    #interpret the score into a readability level
    if fre >= 90:
        level = "Very easy (5th grade)"
    elif fre >= 60:
        level = "Standard (8th-9th grade)"
    elif fre >= 30:
        level = "Difficult (college)"
    else:
        level = "Very difficult (college graduate)"
    #return all readability metrics in a dictionary
    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "syllable_count": syllable_count,
        "flesch_reading_ease": fre,
        "interpretation": level,
    }