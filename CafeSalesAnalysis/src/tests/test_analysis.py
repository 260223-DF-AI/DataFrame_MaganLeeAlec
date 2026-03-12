# ## PyTest testing for analysis.py
# import pytest
# from src.sales_analysis import analysis

# # 1. basic word and character counting, including spaces
# def test_wordcount_basic():
#     text = "Hello Chicago!"
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.word_count() == 2

# def test_charactercount_basic():
#     text = "Chicago"
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.character_count() == 7

# def charactercount_spaces():
#     text = "Hi Chicago"
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.character_count() == 10

# # 2. empty string input
# def test_emptyinput():
#     analyzer = analysis.TextAnalyzer("")
#     assert analyzer.word_count() == 0
#     assert analyzer.character_count() == 0
#     assert analyzer.sentence_count() == 0

# # 3. whitespace only input
# def test_whitespaceonly():
#     analyzer = analysis.TextAnalyzer("   ")
#     assert analyzer.word_count() == 0
#     assert analyzer.character_count() == 3
#     assert analyzer.sentence_count() == 0

# # 4. sentence counting
# def test_sentencecount_basic():
#     text = "Hello Chicago! How are you?"
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.sentence_count() == 2

# # 5. paragraph counting
# def test_paragraphcount():
#     text = "Hello Chicago!\n\nHow are you?"
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.paragraph_count() == 2

# # 6. case insensitivity for word frequency
# def test_wordfrequency_caseinsensitive():
#     text = "Chicago chicago CHICAGO is fun"
#     analyzer = analysis.TextAnalyzer(text)
#     freq = analyzer.word_frequency()
#     assert freq["chicago"] == 3
#     assert freq["fun"] == 1

# # 7. punctuation stripping
# def test_wordfrequency_punctuation():
#     text = "Chicago, chicago! fun."
#     analyzer = analysis.TextAnalyzer(text)
#     freq = analyzer.word_frequency()
#     assert freq["chicago"] == 2
#     assert freq["fun"] == 1

# # 8. most common words
# def test_mostcommonwords():
#     text = "Chicago is so fun.  I love Chicago so much! Chicago has lots to do"
#     analyzer = analysis.TextAnalyzer(text)
#     common = analyzer.most_common_words(2)
#     assert common[0] == ("Chicago", 3) #first most common word is chicago
#     assert common[1] == ("so", 2) #2nd most common word is so

# # 9. average word length
# def test_averagewordlength():
#     text = "Chicago Illinois yay"
#     analyzer = analysis.TextAnalyzer(text)
#     #Chicago: 7.  Illinois: 8.  yay: 3
#     #Average calculation: (7 + 8 + 3) / 3 = 18 / 3 = 6
#     assert analyzer.average_word_length() == pytest.approx(6.0, 0.1)

# # 10. longest word
# def test_longestword():
#     text = "Chicago is so much fun."
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.longest_word() == "Chicago"

# # 11. search/count for a specific word
# def test_countspecificword():
#     text = "Hello Chicago! Chicago is fun. I love Chicago."
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.count_occurances("Chicago") == 3

# # 12. mixed punctuation and new lines
# def test_mixedpunctuation_newlines():
#     text = "Hello Chicago!\n\nChicago is fun. I love Chicago."
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.word_count() == 9
#     assert analyzer.character_count() == 41
#     assert analyzer.sentence_count() == 3
#     assert analyzer.paragraph_count() == 3

# # 13. special characters or numbers
# def test_numbersintext():
#     text = "Chicago zip code is 60601."
#     analyzer = analysis.TextAnalyzer(text)
#     assert analyzer.word_count() == 5

# # 14. invalid input types
# def test_invalidinput_type():
#     with pytest.raises(TypeError):
#         analysis.TextAnalyzer(None)