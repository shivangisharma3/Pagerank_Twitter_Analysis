from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

def insert(arr1,arr2,n,n_id,max_capacity):
	i=len(arr1)-1;
	arr1.append(0);
	arr2.append(0);
	while(i>=0 and arr1[i]>n):		
		arr1[i+1] = arr1[i];
		arr2[i+1] = arr2[i];
		i=i-1
 
	arr1[i+1]= n;
	arr2[i+1]= n_id;
	if(len(arr1)>max_capacity):
		i=max_capacity
		while(i<=len(arr1)-1 and arr1[i]==arr1[max_capacity-1]):
			i=i+1
				
		del arr1[i:]
		del arr2[i:]




def pagerank(tx,top_count):
		for record in tx.run("CALL algo.pageRank.stream('MATCH(u:USER) return id(u) as id','MATCH (u1:USER)-[:TWEETED]->(u2:TWEET)<-[:RETWEET_OF]-(u3:TWEET)<-[:TWEETED]-(u4:USER) RETURN id(u4) as source, id(u1) as target',{graph:'cypher'}) "
	"YIELD node,score with node,score order by score desc limit $top_count " 
	"RETURN node{.id,.name}, score",top_count=top_count):
			print(record["node"]," ",record["score"])




def pagerank5(tx,name,top_count,near_count):
	n_id=[0]
	n_dist=[65565]
	tx.run("MATCH (u1:USER)-[:TWEETED]->(u2:TWEET)<-[:RETWEET_OF]-(u3:TWEET)<-[:TWEETED]-(u4:USER)" 
	"MERGE (u4)-[:pts]->(u1) "  )
	tx.run("MATCH (a:USER),(b:USER) WHERE a.name = 'HRV' AND b.name = 'Ahmet KALKAN' "
		"WITH a,b "
		"MERGE (b)-[:pts]->(a) ")

	for record in tx.run("CALL algo.pageRank.stream('MATCH(u:USER) return id(u) as id','MATCH (u1:USER)-[:TWEETED]->(u2:TWEET)<-[:RETWEET_OF]-(u3:TWEET)<-[:TWEETED]-(u4:USER) RETURN id(u4) as source, id(u1) as target',{graph:'cypher'}) "
	"YIELD node,score with node,score order by score desc limit $top_count " 
	"RETURN node{.id,.name}, score",top_count=top_count):
		print(record["node"]," ",record["score"])
		tmp_id=0
		tmp_dist=0
			
		i=str(record["node"]['name'])

		for rec in tx.run("MATCH (start:USER{name: $name}), (end:USER{name: $i }) " 
		"CALL algo.shortestPath.stream(start, end , null , "
		"{nodeQuery:'USER', relationshipQuery:'pts', defaultValue:1.0, direction:'BOTH'} ) "
		"YIELD nodeId,cost "
		"RETURN nodeId, cost ",i=i ,name=name):
			tmp_id=rec["nodeId"]
			tmp_dist=rec["cost"]

		if(tmp_id!=0):
			insert(n_dist,n_id,tmp_dist,tmp_id,near_count)

#		if(tmp_dist<n_dist[0] and tmp_id!=0):
#			n_id.clear();
#			n_dist.clear();
#			n_dist.append(tmp_dist);
#			n_id.append(tmp_id);
#
#		elif(tmp_dist==n_dist[0] and tmp_id!=0):
#			n_dist.append(tmp_dist);
#			n_id.append(tmp_id);


	if(n_id[0]!=0):
		print("NEARMOST IMPORTANT NODES ARE :-")
		for i in range(len(n_id)): 
			for s in tx.run("MATCH (s) WHERE ID(s)=$a  "
			"RETURN s",a=n_id[i]  ):
				print(s," ","with a distance of",n_dist[i])
	tx.run("MATCH ()-[r:pts]-()" 
	"DELETE r")



num = input('HOW MANY TOTAL POPULAR NODES YOU WANT TO FIND : ')
name = input('Enter a name around which you wish to find popular nodes: ')
num=int(num)
num2 =input('From total popular nodes, how many nodes you wish to find: ')
num2=int(num2)

with driver.session() as session:
	#session.write_transaction(create)
	session.write_transaction(pagerank5,name,num,num2)
	#session.read_transaction(pagerank,num)	






 
