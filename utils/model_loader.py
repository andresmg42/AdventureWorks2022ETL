# etl/model_loader.py
from transformers import MarianTokenizer, MarianMTModel


class ModelRegistry:
    """
    Clase para cargar, almacenar y proporcionar acceso a los modelos de traducción.
    Actúa como caché centralizado.
    """
    def __init__(self):
        self._models = {}
        print("ModelRegistry inicializado.")

    def preload_model(self, src_lang: str, tgt_lang: str):
        """Carga un modelo específico y lo almacena en el registro."""
        model_name = f'Helsinki-NLP/opus-mt-{src_lang}-{tgt_lang}'

        if model_name in self._models:
            print(f"Modelo {model_name} ya está cargado.")
            return

        print(f"--- Loading model {model_name} (this should only happen once). ---")
        try:
            tokenizer = MarianTokenizer.from_pretrained(model_name)
            model = MarianMTModel.from_pretrained(model_name)
            self._models[model_name] = (tokenizer, model)
            print(f"--- Model {model_name} loaded and cached. ---")
        except Exception as e:
            print(f"Error loading model {model_name}: {e}")
            self._models[model_name] = (None, None)

    def get_model(self, src_lang: str, tgt_lang: str):
        """Obtiene un modelo previamente cargado del registro."""
        model_name = f'Helsinki-NLP/opus-mt-{src_lang}-{tgt_lang}'
        return self._models.get(model_name, (None, None))