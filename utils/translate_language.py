import pandas as pd
from transformers import MarianMTModel, MarianTokenizer
from functools import lru_cache # We'll use this for caching

# --- Step 1: Cache the Model ---
# This helper function loads a model and caches it in memory.
# The next time you ask for the same model, it will be returned instantly.
@lru_cache(maxsize=None)
def get_model(model_name):
    """
    Loads and caches a tokenizer and model.
    """
    print(f"--- Loading model {model_name} (this should only happen once) ---")
    try:
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)
        return tokenizer, model
    except Exception as e:
        print(f"Error loading model {model_name}: {e}")
        return None, None

# --- Step 2: The New, Fast Translation Function ---

def convert_language(
    src_name: str,
    tgt_name: str,
    tokenizer,
    model,
    df: pd.DataFrame,
    batch_size: int = 32 # Batches of 32 are often a good starting point
):
    """
    Translates a DataFrame column by only translating the unique values
    and then mapping the results back.
    """
    
    # --- A. Get the cached model ---
    if tokenizer is None:
        print(f"WARNING: There are not model for the column {tgt_name}. Skipping translate process.")
        return df # Failed to load model, just return

    # --- B. The Core Speedup: Translate ONLY Unique Values ---
    # Get all non-null, non-empty unique strings
    mask = df[src_name].notna() & (df[src_name] != '')
    unique_texts = df.loc[mask, src_name].unique().tolist()

    if not unique_texts:
        print("No text to translate.")
        return df

    # --- C. Translate those unique values in batches (your logic) ---
    print(f"Found {len(df)} total rows, but only {len(unique_texts)} unique values to translate.")
    translations = []
    for i in range(0, len(unique_texts), batch_size):
        batch = unique_texts[i : i + batch_size]
        
        # Your original batch-translation logic
        inputs = tokenizer(batch, return_tensors="pt", padding=True, truncation=True)
        translated_tokens = model.generate(**inputs)
        translations.extend(
            [tokenizer.decode(t, skip_special_tokens=True) for t in translated_tokens]
        )

    # --- D. Create the translation map ---
    # This maps "Original Text" -> "Translated Text"
    translation_map = dict(zip(unique_texts, translations))

    # --- E. Apply the map back to the DataFrame (this is instant) ---
    # .map() will apply the translation for all unique values.
    # Anything not in the map (like NaN or '') will become NaN.
    df[tgt_name] = df[src_name].map(translation_map)

    print("Translation complete.")
    return df
