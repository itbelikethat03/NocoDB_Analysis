from transformers import BertTokenizer, BertModel
import torch
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import mysql.connector


def fetch_reviews():
    """Fetch reviews from MySQL."""
    query = """
        SELECT text, stars
        FROM review
        WHERE text IS NOT NULL AND stars IS NOT NULL
        LIMIT 5000
    """
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    connection.close()

    return pd.DataFrame(rows, columns=["text", "stars"])

def preprocess_reviews(df):
    """Simplify ratings to binary classification."""
    df['sentiment'] = df['stars'].apply(lambda x: 1 if x >= 4 else 0 if x <= 2 else None)
    df = df.dropna(subset=['sentiment'])  # Remove neutral ratings
    return df

def encode_texts_with_bert(texts, tokenizer, model):
    """Convert texts to BERT embeddings."""
    embeddings = []
    for text in texts:
        # Tokenize and encode
        inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = model(**inputs)
            # Use the [CLS] token's embedding as the representation
            embeddings.append(outputs.last_hidden_state[:, 0, :].squeeze().numpy())
    return embeddings

def train_with_bert(df):
    """Train a model using BERT embeddings."""
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(df['text'], df['sentiment'], test_size=0.2, random_state=42)

    # Load BERT model and tokenizer
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    model = BertModel.from_pretrained('bert-base-uncased')

    # Encode texts
    print("Encoding training texts with BERT...")
    X_train_embeddings = encode_texts_with_bert(X_train.tolist(), tokenizer, model)
    print("Encoding testing texts with BERT...")
    X_test_embeddings = encode_texts_with_bert(X_test.tolist(), tokenizer, model)

    # Train logistic regression on embeddings
    clf = LogisticRegression(max_iter=1000)
    clf.fit(X_train_embeddings, y_train)

    # Evaluate the model
    y_pred = clf.predict(X_test_embeddings)
    print("Classification Report:")
    print(classification_report(y_test, y_pred))

if __name__ == "__main__":
    # Fetch and preprocess reviews
    print("Fetching reviews from MySQL...")
    reviews_df = fetch_reviews()
    print(f"Fetched {len(reviews_df)} reviews.")
    reviews_df = preprocess_reviews(reviews_df)

    # Train and evaluate model
    print("Training sentiment analysis model with BERT embeddings...")
    train_with_bert(reviews_df)
