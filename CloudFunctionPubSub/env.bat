gcloud functions deploy funcPubSub --entry-point=cloudpubsub.PubSubFunction --runtime=java11 --trigger-topic translation --source=target/deployment --service-account=firestore-translation@serious-fabric-252921.iam.gserviceaccount.com
cls
cmd