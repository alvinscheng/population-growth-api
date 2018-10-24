const redis = require('redis');
const client = redis.createClient();
const request = require('request');
const csv = require('csvtojson')

client.on('connect', function() {
  console.log('Redis client connected');
});
client.on('error', function (err) {
  console.log('Error ' + err);
});



csv()
.fromStream(request.get('https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/cbsa_to_msa.csv'))
.subscribe((json)=>{
  return new Promise((resolve, reject)=>{
    if (json.LSAD === 'Metropolitan Statistical Area') {
      client.set('CBSA-' + json.CBSA, JSON.stringify(json));

      if (json.MDIV) {
        client.set('MDIV-' + json.MDIV, json.CBSA);
      }
    }
    resolve()
  })
},
(err) => console.log('err: ', err),
() => {

})

csv()
  .fromStream(request.get('https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/zip_to_cbsa.csv'))
  .subscribe((json)=>{
    return new Promise((resolve, reject)=>{
      client.set('ZIP-' + json.ZIP, json.CBSA);
      resolve()
    })
  },
  (err) => console.log('err: ', err),
  () => {

  })

let zip = 32003
client.get('ZIP-' + zip, function (error, CBSA) {
  if (error) {
      console.log(error);
      throw error;
  }

  client.get('MDIV-' + CBSA, function (error, newCBSA) {
    if (error) {
        console.log(error);
        throw error;
    }

    const updatedCBSA = newCBSA || CBSA;
    
    client.get('CBSA-' + updatedCBSA, function (error, data) {
      if (error) {
          console.log(error);
          throw error;
      }

      data = JSON.parse(data)
      console.log({
        Zip: zip,
        CBSA: CBSA,
        MSA: data.NAME,
        Pop2015: data.POPESTIMATE2015,
        Pop2014: data.POPESTIMATE2014
      });
    });
  });
});





// // 1. [Zip to CBSA (csv)](https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/zip_to_cbsa.csv)
// // 1. [CBSA to MSA (csv)](https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/cbsa_to_msa.csv)