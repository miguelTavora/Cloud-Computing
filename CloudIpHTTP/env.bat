gcloud functions deploy funcHttpIP --entry-point=functionhttp.HttpIpProvider --runtime=java11 --trigger-http --source=target/deployment --service-account=compute-engine-service@serious-fabric-252921.iam.gserviceaccount.com
cls
cmd