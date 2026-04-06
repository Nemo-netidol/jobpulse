
CATEGORY_KEYWORDS = {
    "AI Engineer": ["machine learning", "deep learning", "llm", "ai", "neural", "pytorch", "tensorflow"],
    "Data Engineer": ["pipeline", "etl", "airflow", "spark", "bigquery", "dbt", "kafka"],
    "MLOps": ["mlflow", "kubeflow", "model serving", "ci/cd", "deployment", "docker", "kubernetes"],
    "Data Scientist": ["statistics", "analysis", "hypothesis", "experiment", "r ", "pandas"],
}

def classify_category(title: str, description: str):

    title = title or ""
    description = description or ""
    text = f"{title} {description}".lower()
   
    scores = {
     category: sum(1 for kw in keywords if kw in text) 
     for category, keywords in CATEGORY_KEYWORDS.items()
     }
    
    best_match = max(scores, key=scores.get)

    return best_match if scores[best_match] > 0 else "Other"
