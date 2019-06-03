import json
import boto3

def describe_volumes(client, volume_ids):
        volume_response=client.describe_volumes(VolumeIds=volume_ids)['Volumes']
        print(volume_response)
        volume_dict = {}
        for item in volume_response:
            volume_dict[item['VolumeId']] = item['Size']
        return (volume_dict)
        
def decribe_instances(client,instance_ids):
        instance_ami_dict = {}
        ec2_image_response= client.describe_instances(InstanceIds=instance_ids)['Reservations']
        for item in ec2_image_response:
            image_item=item['Instances']
            for i in image_item:
                instance_ami_dict[i['InstanceId']] = i['ImageId']
        return (instance_ami_dict)    
    
def lambda_handler(event, context):
        client = boto3.client('emr')
        ec2_client = boto3.client('ec2')
        clusterId=[]
        clusterName=[]
        
        list_clusters_response = client.list_clusters(ClusterStates=['WAITING'])['Clusters']
    
        for item in list_clusters_response:
            clusterId.append(item['Id'])
        
        volumeIds = []
        tuples = []
        is_instances_id =[]
        instanceIds =[]
        for entry in clusterId:
            list_instances_response = client.list_instances(ClusterId=entry)['Instances']
            cluster_name_response = client.describe_cluster(ClusterId=entry)['Cluster']

            
            for item in list_instances_response:
                instance_type=item['InstanceType']
                instance_id=(item['Ec2InstanceId'])
                volume_id=item['EbsVolumes'][0]['VolumeId']
                
                volumeIds.append(volume_id)
                instanceIds.append(instance_id)
                
                tuples.append((cluster_name_response['Name'], entry, instance_id, instance_type, len(list_instances_response), volume_id))
                
        volume_test = describe_volumes(ec2_client, volumeIds)
        instance_test = decribe_instances(ec2_client, instanceIds)
        
        for item in tuples:
            print(str(item[0]) + ", " + str(item[1]) + ", " + str(item[2])+ "," + str(instance_test[item[2]]) + ", " + str(item[3]) + ", " + str(item[4]) + ", " + str(item[5]) + "," + str(volume_test[item[5]]))
            #if 'cra' not in str(item[0]):
                #is_instances_id.append(str(item[2]))
        
