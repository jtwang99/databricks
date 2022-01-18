# Databricks notebook source
# MAGIC %md ## 2. Do distributed model inference from Delta
# MAGIC ---
# MAGIC 
# MAGIC * Start from the Delta table `dbfs:/databricks-datasets/flowers/`, which is a copy of the output table of the ETL image dataset in a Delta table notebook.
# MAGIC * Use scalar iterator Pandas UDF to make batch predictions.

# COMMAND ----------

import io

from tensorflow.keras.applications.imagenet_utils import decode_predictions
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms
from PIL import Image

# COMMAND ----------

# MAGIC %md ###Define a Dataset that processes the input

# COMMAND ----------

class ImageNetDataset(Dataset):
  """
  Converts image contents into a PyTorch Dataset with standard ImageNet preprocessing.
  """
  def __init__(self, contents):
    self.contents = contents

  def __len__(self):
    return len(self.contents)

  def __getitem__(self, index):
    return self._preprocess(self.contents[index])

  def _preprocess(self, content):
    """
    Preprocesses the input image content using standard ImageNet normalization.
    
    See https://pytorch.org/docs/stable/torchvision/models.html.
    """
    image = Image.open(io.BytesIO(content))
    transform = transforms.Compose([
      transforms.Resize(256),
      transforms.CenterCrop(224),
      transforms.ToTensor(),
      transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    return transform(image)

# COMMAND ----------

# MAGIC %md ###Define a Pandas UDF for the inference task
# MAGIC 
# MAGIC There are three UDFs in PySpark that provides 1:1 mapping semantic:
# MAGIC * PySpark UDF: record -> record, performance issues in data serialization, not recommended
# MAGIC * Scalar Pandas UDF: pandas Series/DataFrame -> pandas Series/DataFrame, no shared states among batches
# MAGIC * **Scalar iterator Pandas UDF**: initialize some state first, then go through batches.
# MAGIC 
# MAGIC Databricks recommends scalar iterator Pandas UDF for model inference.

# COMMAND ----------

def imagenet_model_udf(model_fn):
  """
  Wraps an ImageNet model into a Pandas UDF that makes predictions.
  
  You might consider the following customizations for your own use case:
    - Tune DataLoader's batch_size and num_workers for better performance.
    - Use GPU for acceleration.
    - Change prediction types.
  """
  def predict(content_series_iter):
    model = model_fn()
    model.eval()
    for content_series in content_series_iter:
      dataset = ImageNetDataset(list(content_series))
      loader = DataLoader(dataset, batch_size=64)
      with torch.no_grad():
        for image_batch in loader:
          predictions = model(image_batch).numpy()
          predicted_labels = [x[0] for x in decode_predictions(predictions, top=1)]
          yield pd.DataFrame(predicted_labels)
  return_type = "class: string, desc: string, score:float"
  return pandas_udf(return_type, PandasUDFType.SCALAR_ITER)(predict)

# COMMAND ----------

# Wraps MobileNetV2 as a Pandas UDF.
mobilenet_v2_udf = imagenet_model_udf(lambda: models.mobilenet_v2(pretrained=True))

# COMMAND ----------

# MAGIC %md ###Do distributed inference in DataFrames API
# MAGIC 
# MAGIC * Specify the required columns and how to compute each.
# MAGIC * Let Spark optimize the execution instead of writing imperative RDD code.
# MAGIC * To automatically apply inference to new data in the Delta table, use `spark.readStream` to load the Delta table as a stream source, and write the predictions to another Delta table.

# COMMAND ----------

images = spark.read.format("delta") \
  .load("/databricks-datasets/flowers/delta") \
  .limit(100)  # use a subset to reduce the running time for demonstration purpose
predictions = images.withColumn("prediction", mobilenet_v2_udf(col("content")))

# COMMAND ----------

display(predictions.select(col("path"), col("prediction")).limit(5))

# COMMAND ----------

# MAGIC %md ### You can do more (optional)

# COMMAND ----------

# MAGIC %md Filter images to predict based on some metadata.

# COMMAND ----------

filtered = predictions \
  .filter((col("label") == "daisy") & (col("size.width") >= 300)) \
  .select(col("path"), col("label"), col("prediction"))
display(filtered.limit(5))

# COMMAND ----------

# MAGIC %md Compare predictions from two models.

# COMMAND ----------

# Define model_fn for ResNet50 by downloading and broadcasting pretrained weights and then reconstructing the model on workers.
# This is an alternative to the above approach that downloads pretrained weights directly on workers.
# While both work, the boradcast approach does not require model access from workers.

resnet50 = models.resnet50(pretrained=True)
bc_resnet50_state = sc.broadcast(resnet50.state_dict())

def resnet50_fn():
  """
  Reconstructs ResNet50 on workers using broadcasted weights.
  """
  model = models.resnet50(pretrained=False)
  model.load_state_dict(bc_resnet50_state.value)
  return model

resnet50_udf = imagenet_model_udf(resnet50_fn)

# COMMAND ----------

predictions2 = images.select(
  col("path"),
  mobilenet_v2_udf(col("content")),
  resnet50_udf(col("content")))
display(predictions2.limit(5))

# COMMAND ----------

# MAGIC %md Do inference directly in SQL.

# COMMAND ----------

images.createOrReplaceTempView("tmp_flowers")
spark.udf.register("mobilenet_v2", mobilenet_v2_udf)
spark.udf.register("resnet50", resnet50_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT path, label, mobilenet_v2(content), resnet50(content)
# MAGIC FROM tmp_flowers
# MAGIC WHERE label = "daisy"
# MAGIC LIMIT 5

# COMMAND ----------


