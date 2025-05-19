# create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TextPreprossing").getOrCreate()


# load article data
# needed columns: "articleID", "documentType", "headline", "pubDate", "snippet", "webURL", "file_name"
article_df = spark.read.option("header", True).csv("pre_data/all_articles.csv").select("articleID", "documentType", "headline", "pubDate", "snippet", "webURL", "file_name")

# replace unwanted characters and convert text to lowercase
from pyspark.sql.functions import lower, col, regexp_replace ,when,lit
article_df_clean = article_df.withColumn("cleaned_snippet", lower(regexp_replace(col("snippet"), "[^a-zA-Z0-9\\s]", "")))

# Tokenize the Text
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="cleaned_snippet", outputCol="tokens")
article_df_tokenized = tokenizer.transform(article_df_clean)

# Remove Stop Words
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
article_df_filtered = remover.transform(article_df_tokenized)

# data write is remaining
file_names = ["01_2017", "02_2017", "03_2017", "04_2017", "05_2017", "01_2018", "02_2018", "03_2018", "04_2018"]
for file_name in file_names:
    filtered_df = article_df_filtered.filter(article_df_filtered['file_name'] == file_name)
    filtered_df.write.mode("overwrite").json(f"output_data/articles/{file_name}")


#needed columns: "commentID" ,"commentTitle", "commentBody", "createDate", "userID", ""userDisplayName","file_name"
comments_df = spark.read.option("header",True).csv("pre_data/all_comments.csv").select(
    "commentID", "commentTitle", "commentBody", "createDate", "userID", "userDisplayName", "file_name"
)

comments_df_clean = comments_df.withColumn(
    "cleaned_commentBody",
    when(col("commentBody").isNull() | (col("commentBody") == ""), lit(""))
    .otherwise(lower(regexp_replace(col("commentBody"), "[^a-zA-Z0-9\\s]", "")))
)

tokenizer = Tokenizer(inputCol="cleaned_commentBody",outputCol="comment_tokens")
comments_tokenized = tokenizer.transform(comments_df_clean)

remover = StopWordsRemover(inputCol="comment_tokens",outputCol="filtered_comment_tokens")
comments_filtered = remover.transform(comments_tokenized)

file_names = ["01_2017", "02_2017", "03_2017", "04_2017", "05_2017", "01_2018", "02_2018", "03_2018", "04_2018"]
for file_name in file_names:
    filtered_df = comments_filtered.filter(comments_filtered['file_name'] == file_name)
    filtered_df.write.mode('overwrite').json(f"output_data/comments/{file_name}")