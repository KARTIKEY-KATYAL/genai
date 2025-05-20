from fastapi import FastAPI, UploadFile, Path, HTTPException
from app.utils.file import save_to_disk
from .db.collections.files import files_collection, FileSchema
from .queue.q import q
from .queue.workers import process_file
from bson import ObjectId
from bson.errors import InvalidId

app = FastAPI()


@app.get("/")
def hello():
    return {"status": "healthy"}


@app.get("/files/{id}")
async def get_file_by_id(id: str = Path(..., description="ID of the file")):
    try:
        db_file = await files_collection.find_one({"_id": ObjectId(id)})
        
        if not db_file:
            raise HTTPException(status_code=404, detail="File not found")
        
        return {
            "_id": str(db_file["_id"]),
            "name": db_file["name"],
            "status": db_file["status"],
            "result": db_file["result"] if "result" in db_file else None,
        }
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid file ID format")


@app.post("/upload")
async def upload_file(
    file: UploadFile
):

    db_file = await files_collection.insert_one(
        document=FileSchema(
            name=file.filename,
            status="saving"
        )
    )

    file_path = f"/mnt/uploads/{str(db_file.inserted_id)}/{file.filename}"

    await save_to_disk(file=await file.read(), path=file_path)

    # Push to Queue
    q.enqueue(process_file, str(db_file.inserted_id), file_path)

    # MongoDB Save
    await files_collection.update_one({"_id": db_file.inserted_id}, {
        "$set": {
            "status": "queued"
        }
    })

    return {"file_id": str(db_file.inserted_id)}
