from collections import Counter, defaultdict, namedtuple
from .models import WordFrequency, NGram, DocumentStats, AnalysisResult
from .tokenizer import tokenize, get_sentences, get_ngrams, remove_stopwords

class TextAnalyzer:
    """Analyzes text documents for various metrics."""
    
    def __init__(self, text):
        self.text = text
        self.words = tokenize(text)
        self.sentences = get_sentences(text)
        self.word_counter = Counter(self.words)
    
    def get_word_frequencies(self, top_n=20, exclude_stopwords=True):
        """
        Get top N word frequencies.
        Returns: List of WordFrequency namedtuples
        """
        WordFrequency = namedtuple("WordFrequency", "x")
        # first get the counters for the word
        stopwords_rm_words = remove_stopwords(self.words)
        stopwords_rm_counter = Counter(stopwords_rm_words)

        if exclude_stopwords:
            sorted_counter = stopwords_rm_counter.most_common(top_n)
            return [WordFrequency(counter) for counter in sorted_counter]
        else:
            sorted_counter = self.word_counter.most_common(top_n)
            return [WordFrequency(counter) for counter in sorted_counter]
        
    
    def get_bigrams(self, top_n=10):
        """
        Get top N bigrams (2-word phrases).
        Returns: List of NGram namedtuples
        """
        bigram_list = []
        Bigram = namedtuple("Bigram", ("x", "y"))
        sorted_counter = self.word_counter.most_common(top_n)
        for i in range(len(self.words) - 2 + 1):
            word = self.words[i]
            bigram_list.append(Bigram(self.words[i], self.words[i+1]))
        return bigram_list
        
    
    def get_trigrams(self, top_n=10):
        """
        Get top N trigrams (3-word phrases).
        Returns: List of NGram namedtuples
        """
        trigram_list = []
        Trigram = namedtuple("Trigram", ("x", "y", "z"))
        sorted_counter = self.word_counter.most_common(top_n)
        for i in range(len(self.words) - 3 + 1):
            word = self.words[i]
            trigram_list.append(Trigram(self.words[i], self.words[i+1], self.words[i+2]))
        return trigram_list
        
    
    def get_document_stats(self):
        """
        Calculate overall document statistics.
        Returns: DocumentStats namedtuple
        """
        print("=== Text Analysis Report ===")
        print("Document Statistics: ")
        
        pass
    
    def get_word_length_distribution(self):
        """
        Group words by length.
        Returns: defaultdict mapping length -> list of words
        """
        pass
    
    def analyze(self):
        """
        Run complete analysis.
        Returns: AnalysisResult namedtuple
        """
        pass



if __name__ == "__main__":
    textanalyzer = TextAnalyzer("Sample Sample Sample text lorem  lorem ipsum slay ipsum")
    #print(textanalyzer.get_word_frequencies(top_n=3))
    #print(textanalyzer.get_bigrams())
    print(textanalyzer.get_trigrams())
