from typing import List, Union, Dict, Any
from pydantic import BaseModel

from config.models import FraudRankConfig    


class BaseRequest(BaseModel):
    orgs: List[str]
    periods: Union[str, List[str]]
    params: Dict[str, Any]


class FraudRankRequest(BaseRequest):
    params: FraudRankConfig