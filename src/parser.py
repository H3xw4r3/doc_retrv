import os
from database import Database
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer


class DocumentParser:

    stop_words = stopwords.words('english')
    porter = PorterStemmer()

    def __init__(self, host, user, password):
        self.db = Database(host, user, password)

    def parse(self, directory, file_extension_list):
        database = input("Enter database to use: ")
        self.db.create_db(database)
        files = [file for file in os.listdir(directory) if file.endswith(file_extension_list)]

        for file in files:

            path = os.path.join(directory, file)     # Joins file name with the directory so file can be accessed
            with open(path, 'r') as file_cont:
                file_content = file_cont.read().split()
                for word in file_content:
                    word = word.lower()
                    stem_word = DocumentParser.porter.stem(word)
                    if stem_word not in DocumentParser.stop_words:
                        self.db.insert('Words', term=stem_word)
                        word_id = self.db.retrieve('Words', 'id', term=stem_word)[0][0]
                        self.db.insert('Documents', path=path)
                        doc_id = self.db.retrieve('Documents', 'id', path=path)[0][0]
                        print(f"Word id: {word_id}")
                        print(f"Document id: {doc_id}")
                        self.db.insert('InvertedIndex', word_id=word_id, doc_id=doc_id)
            self.db.commit()