from pyspark.ml.feature import Tokenizer, Word2Vec
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from sklearn.metrics.pairwise import cosine_similarity


# Initialize Spark session
def performSemanticSearch(mainDf, userQueryDf):
    print("running semantic search")
    tokenizer = Tokenizer(inputCol="INVENTION_TITLE", outputCol="words_INVENTION_TITLE")
    mainDf = tokenizer.transform(mainDf)

    tokenizer = Tokenizer(inputCol="ABSTRACT_OF_INVENTION", outputCol="words_ABSTRACT_OF_INVENTION")
    mainDf = tokenizer.transform(mainDf)

    tokenizer = Tokenizer(inputCol="COMPLETE_SPECIFICATION", outputCol="words_COMPLETE_SPECIFICATION")
    mainDf = tokenizer.transform(mainDf)

    # Train Word2Vec model for each column
    word2Vec_col1 = Word2Vec(vectorSize=100, minCount=0, inputCol="words_INVENTION_TITLE",
                             outputCol="embedding_INVENTION_TITLE")
    model_col1 = word2Vec_col1.fit(mainDf)
    mainDf = model_col1.transform(mainDf)

    word2Vec_col2 = Word2Vec(vectorSize=100, minCount=0, inputCol="words_ABSTRACT_OF_INVENTION",
                             outputCol="embedding_ABSTRACT_OF_INVENTION")
    model_col2 = word2Vec_col2.fit(mainDf)
    mainDf = model_col2.transform(mainDf)

    word2Vec_col3 = Word2Vec(vectorSize=100, minCount=0, inputCol="words_COMPLETE_SPECIFICATION",
                             outputCol="embedding_COMPLETE_SPECIFICATION")
    model_col3 = word2Vec_col3.fit(mainDf)
    mainDf = model_col3.transform(mainDf)

    # Tokenize and transform the user query for each specified column

    tokenizer = Tokenizer(inputCol="INVENTION_TITLE", outputCol="words_INVENTION_TITLE")
    userQueryDf = tokenizer.transform(userQueryDf)

    tokenizer = Tokenizer(inputCol="ABSTRACT_OF_INVENTION", outputCol="words_ABSTRACT_OF_INVENTION")
    userQueryDf = tokenizer.transform(userQueryDf)

    tokenizer = Tokenizer(inputCol="COMPLETE_SPECIFICATION", outputCol="words_COMPLETE_SPECIFICATION")
    userQueryDf = tokenizer.transform(userQueryDf)

    userQueryDf = model_col1.transform(userQueryDf)
    userQueryDf = model_col2.transform(userQueryDf)
    userQueryDf = model_col3.transform(userQueryDf)

    # Extract the user query embeddings for each column
    user_query_embedding_col1 = \
        userQueryDf.select("embedding_INVENTION_TITLE").rdd.map(lambda x: x[0].toArray()).collect()[0]
    user_query_embedding_col2 = \
        userQueryDf.select("embedding_ABSTRACT_OF_INVENTION").rdd.map(lambda x: x[0].toArray()).collect()[0]
    user_query_embedding_col3 = \
        userQueryDf.select("embedding_COMPLETE_SPECIFICATION").rdd.map(lambda x: x[0].toArray()).collect()[0]

    # Calculate cosine similarity for each column
    cosine_similarity_udf_col1 = udf(lambda x: float(cosine_similarity([user_query_embedding_col1], [x])[0]),
                                     FloatType())
    df_result_col1 = mainDf.withColumn("similarity_col1", cosine_similarity_udf_col1(col("embedding_INVENTION_TITLE")))

    cosine_similarity_udf_col2 = udf(lambda x: float(cosine_similarity([user_query_embedding_col2], [x])[0]),
                                     FloatType())
    df_result_col2 = mainDf.withColumn("similarity_col2",
                                       cosine_similarity_udf_col2(col("embedding_ABSTRACT_OF_INVENTION")))

    cosine_similarity_udf_col3 = udf(lambda x: float(cosine_similarity([user_query_embedding_col3], [x])[0]),
                                     FloatType())
    df_result_col3 = mainDf.withColumn("similarity_col3",
                                       cosine_similarity_udf_col3(col("embedding_COMPLETE_SPECIFICATION")))

    similarity_threshold = 0.6

    # Filter DataFrame based on similarity threshold for each column
    resultFilteredCol1 = df_result_col1.filter(df_result_col1.similarity_col1 > similarity_threshold)
    resultFilteredCol2 = df_result_col2.filter(df_result_col2.similarity_col2 > similarity_threshold)
    resultFilteredCol3 = df_result_col3.filter(df_result_col3.similarity_col3 > similarity_threshold)

    # print("Result for col1:")
    # result_filtered_col1.show()
    #
    print("Result for col2:")
    resultFilteredCol2.select("INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION").show()

    print("Result for col3:")
    resultFilteredCol3.select("INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION").show()

    resultDF = resultFilteredCol1.join(resultFilteredCol2,
                                       resultFilteredCol1.APPLICATION_NUMBER == resultFilteredCol2.APPLICATION_NUMBER,
                                       "right").drop(resultFilteredCol1.INVENTION_TITLE,
                                                     resultFilteredCol1.APPLICATION_FILING_DATE,
                                                     resultFilteredCol1.FIELD_OF_INVENTION,
                                                     resultFilteredCol1.ABSTRACT_OF_INVENTION,
                                                     resultFilteredCol1.COMPLETE_SPECIFICATION,
                                                     resultFilteredCol1.APPLICATION_NUMBER,
                                                     resultFilteredCol1.TITLE_SCORE,
                                                     resultFilteredCol1.ABSTARCT_SCORE,
                                                     resultFilteredCol1.COMSPEC_SCORE,
                                                     resultFilteredCol1.words_INVENTION_TITLE,
                                                     resultFilteredCol1.words_ABSTRACT_OF_INVENTION,
                                                     resultFilteredCol1.words_COMPLETE_SPECIFICATION,
                                                     resultFilteredCol1.embedding_INVENTION_TITLE,
                                                     resultFilteredCol1.embedding_ABSTRACT_OF_INVENTION,
                                                     resultFilteredCol1.embedding_COMPLETE_SPECIFICATION,
                                                     resultFilteredCol2.TITLE_SCORE,
                                                     resultFilteredCol2.ABSTARCT_SCORE,
                                                     resultFilteredCol2.COMSPEC_SCORE,
                                                     resultFilteredCol2.words_INVENTION_TITLE,
                                                     resultFilteredCol2.words_ABSTRACT_OF_INVENTION,
                                                     resultFilteredCol2.words_COMPLETE_SPECIFICATION,
                                                     resultFilteredCol2.embedding_INVENTION_TITLE,
                                                     resultFilteredCol2.embedding_ABSTRACT_OF_INVENTION,
                                                     resultFilteredCol2.embedding_COMPLETE_SPECIFICATION
                                                     )

    resultDF = resultDF.join(resultFilteredCol3, resultFilteredCol3.APPLICATION_NUMBER == resultDF.APPLICATION_NUMBER,
                             "left").drop(resultDF.INVENTION_TITLE,
                                          resultDF.APPLICATION_FILING_DATE, resultDF.FIELD_OF_INVENTION,
                                          resultDF.ABSTRACT_OF_INVENTION, resultDF.COMPLETE_SPECIFICATION,
                                          resultDF.APPLICATION_NUMBER,
                                          resultFilteredCol3.words_INVENTION_TITLE,
                                          resultFilteredCol3.words_ABSTRACT_OF_INVENTION,
                                          resultFilteredCol3.words_COMPLETE_SPECIFICATION,
                                          resultFilteredCol3.embedding_INVENTION_TITLE,
                                          resultFilteredCol3.embedding_ABSTRACT_OF_INVENTION,
                                          resultFilteredCol3.embedding_COMPLETE_SPECIFICATION,
                                          resultFilteredCol3.TITLE_SCORE,
                                          resultFilteredCol3.ABSTARCT_SCORE,
                                          resultFilteredCol3.COMSPEC_SCORE,
                                          )
    print("Result for join of three are:")
    resultDF.select("APPLICATION_NUMBER", "INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION",
                    "FIELD_OF_INVENTION", "similarity_col1", "similarity_col2", "similarity_col3").sort(
        "similarity_col1", "similarity_col2", "similarity_col3").show()
