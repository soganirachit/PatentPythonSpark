import os

from pyspark.sql import SparkSession

from imports import semanticAnalysis as sa
from imports import writeFile as wf

class DataTransformation:
    os.environ["HADOOP_HOME"] = "C:/spark-3.5.0-bin-hadoop3/"

    def __init__(self):
        self.csvFilePath = r"E:\CaptchaImages\NewDownload\output.csv"
        self.spark = SparkSession.builder.master("local") \
            .appName('TestOracleJdbc') \
            .config("spark.jars",
                    r"C:\Users\rachit sogani\.m2\repository\com\oracle\database\jdbc\ojdbc8\21.11.0.0\ojdbc8-21.11.0.0.jar") \
            .getOrCreate()

    def remove_stop_words(self,input_text):
        # List of common English stop words
        stop_words = set(
            ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
             'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
             'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
             'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
             'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but',
             'if',
             'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between',
             'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out',
             'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why',
             'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
             'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't",
             'should',
             "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't",
             'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn',
             "isn't",
             'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn',
             "shouldn't",
             'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't", 'also'])

        # Tokenize the input text into words
        words = input_text.split()

        # Remove stop words from the list of words
        filtered_words = [word for word in words if word.lower() not in stop_words]

        # Join the filtered words to form a string
        filtered_text = ' '.join(filtered_words)

        return filtered_text

    def fetchData(self,inputKeywords, inputFieldOfInvention):
        wf.writeFileMethod(inputKeywords, self.csvFilePath)

        # containsKeywords = remove_stop_words(inputKeywords)
        containsKeywords = "'" + ("|".join(self.remove_stop_words(inputKeywords).split(" "))).strip("|") + "'"
        print("containsKeywords is " + containsKeywords)

        inputFieldOfInvention = self.remove_stop_words(inputFieldOfInvention).split(" ")
        fieldOfInvention = ""
        for word in list(inputFieldOfInvention):
            fieldOfInvention += "'" + word + "',"
        fieldOfInvention = fieldOfInvention.strip(",")
        print("fieldOfInvention is " + fieldOfInvention)

        self.spark.conf.set("spark.sql.oracle.enabled", "true")
        query = (
                    "SELECT INVENTION_TITLE,APPLICATION_NUMBER, APPLICATION_FILING_DATE, FIELD_OF_INVENTION, ABSTRACT_OF_INVENTION, "
                    "COMPLETE_SPECIFICATION, title_score, abstarct_score, comSpec_score FROM ( SELECT INVENTION_TITLE,APPLICATION_NUMBER,"
                    " APPLICATION_FILING_DATE, FIELD_OF_INVENTION, ABSTRACT_OF_INVENTION, COMPLETE_SPECIFICATION, CONTAINS(INVENTION_TITLE,"
                    " ") + containsKeywords + " ) as title_score, CONTAINS(ABSTRACT_OF_INVENTION," + containsKeywords + (
                    " ) as abstarct_score,"
                    " CONTAINS(COMPLETE_SPECIFICATION, ") + containsKeywords + (
                    " ) as comSpec_score FROM patent_details )subquery "
                    "WHERE (title_score > 0 or abstarct_score > 0 or comSpec_score > 0) "
                    "ORDER BY title_score DESC,abstarct_score DESC,comSpec_score DESC FETCH FIRST 10 ROWS ONLY")
        print(query)

        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("query", query) \
            .option("user", "sys as sysdba") \
            .option("password", "sys123") \
            .load()

        userQueryDf = self.spark.read.format("csv").option("header", True).load(
            self.csvFilePath)
        # userQueryDf.show()

        df.select("INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION").show()
        sa.performSemanticSearch(df, userQueryDf)
        print("main data frame is")
        df.select("INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION").show()
        return df.toJSON().collect()

    # fetchData("car that runs on electricity and has a range of 400 km ", "")
