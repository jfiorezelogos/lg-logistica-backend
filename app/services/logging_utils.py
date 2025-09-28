# app/services/logging_utils.py
import uuid
import contextvars

# ContextVar global para guardar o correlation_id
correlation_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)

def set_correlation_id(value: str | None = None) -> str:
    """
    Define (ou gera) o correlation_id para o contexto atual.
    Retorna o valor definido.
    """
    cid = value or str(uuid.uuid4())
    correlation_id_ctx.set(cid)
    return cid

def get_correlation_id() -> str:
    """
    Recupera o correlation_id atual do contexto.
    Se nenhum valor foi definido ainda, retorna string vazia.
    """
    return correlation_id_ctx.get()
