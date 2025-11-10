from deep_translator import GoogleTranslator


def to_spanish(df, df_column: str) -> dict:
    translator = GoogleTranslator(source='auto', target='es')

    values_map = {}
    for value in df[df_column].unique():
        try:
            translated = translator.translate(value)
        except Exception:
            translated = value
        values_map[value] = translated

    return values_map
