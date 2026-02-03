from pydantic import BaseModel

class Feedback(BaseModel):
    source_file_name: str
    candidate_file_name: str
    label: int