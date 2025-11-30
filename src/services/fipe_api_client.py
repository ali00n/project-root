import requests
import time
from typing import List, Dict, Optional, Any

BASE_URL = "https://parallelum.com.br/fipe/api/v1/motos"


class FipeApiClient:
    def __init__(self, retries: int = 3, timeout: int = 10, delay: float = 0.8):
        """
        :param retries: número de tentativas caso a API falhe
        :param timeout: tempo limite por requisição
        :param delay: tempo de espera entre cada requisição (evita bloqueio)
        """
        self.retries = retries
        self.timeout = timeout
        self.delay = delay

    # =======================================================
    #   MÉTODO INTERNO PARA CHAMAR A API COM RETRY
    # =======================================================
    def _get(self, url: str) -> Optional[Any]:
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, timeout=self.timeout)

                if resp.status_code == 200:
                    time.sleep(self.delay)  # Delay entre requisições
                    return resp.json()
                else:
                    print(f"[FIPE] status {resp.status_code} para URL {url}")

            except Exception as e:
                print(f"[FIPE] ERRO {e} para URL {url}")

            time.sleep(self.delay)

        print(f"[FIPE] Falha após {self.retries} tentativas → {url}")
        return None

    # =======================================================
    #   ENDPOINTS DA API
    # =======================================================
    def get_marcas(self) -> List[Dict[str, Any]]:
        """Retorna lista de todas as marcas de motos."""
        return self._get(f"{BASE_URL}/marcas") or []

    def get_modelos(self, marca_codigo: str) -> List[Dict[str, Any]]:
        """Retorna lista de modelos de uma marca."""
        data = self._get(f"{BASE_URL}/marcas/{marca_codigo}/modelos")
        if not data:
            return []
        return data.get("modelos", [])

    def get_anos(self, marca_codigo: str, modelo_codigo: str) -> List[Dict[str, Any]]:
        """Retorna lista dos anos disponíveis para um modelo."""
        return self._get(f"{BASE_URL}/marcas/{marca_codigo}/modelos/{modelo_codigo}/anos") or []

    def get_preco(self, marca_codigo: str, modelo_codigo: str, ano_codigo: str) -> Optional[Dict[str, Any]]:
        """Retorna o preço FIPE de um modelo/ano específico."""
        return self._get(f"{BASE_URL}/marcas/{marca_codigo}/modelos/{modelo_codigo}/anos/{ano_codigo}")


# =======================================================
#   PARSE DO VALOR FIPE (R$ 24.510,00 → 24510.00)
# =======================================================
def parse_valor_fipe(v: Optional[str]) -> Optional[float]:
    if not v:
        return None
    try:
        s = (
            v.replace("R$", "")
             .replace(".", "")
             .replace(",", ".")
             .strip()
        )
        return float(s)
    except:
        return None
