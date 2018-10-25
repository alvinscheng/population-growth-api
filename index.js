require('dotenv').config()
const express = require('express')
const redis = require('redis')
const request = require('request')
const csv = require('csvtojson')

const app = express()
const client = redis.createClient(process.env.REDIS_URL)

client.on('connect', function() {
  console.log('Redis client connected')
})
client.on('error', function (err) {
  console.log('Redis client error ' + err)
})

app.get('/', (req, res) => {
  const { zip } = req.query

  client.get('ZIP-' + zip, function (error, CBSA) {
    if (!CBSA) {
      res.sendStatus(404)
    } else {
      client.get('MDIV-' + CBSA, function (error, newCBSA) {
        const updatedCBSA = newCBSA || CBSA

        client.get('CBSA-' + updatedCBSA, function (error, data) {
          data = JSON.parse(data)
          res.json({
            Zip: zip,
            CBSA: CBSA,
            MSA: data ? data.NAME : 'N/A',
            Pop2015: data ? data.POPESTIMATE2015 : 'N/A',
            Pop2014: data ? data.POPESTIMATE2014 : 'N/A'
          })
        })
      })
    }
  })
})

const PORT = 
app.listen(PORT, () => {
  csv()
    .fromStream(request.get('https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/cbsa_to_msa.csv'))
    .subscribe((json)=>{
      return new Promise((resolve, reject)=>{
        if (json.LSAD === 'Metropolitan Statistical Area') {
          client.set('CBSA-' + json.CBSA, JSON.stringify(json))

          if (json.MDIV) {
            client.set('MDIV-' + json.MDIV, json.CBSA)
          }
        }
        resolve()
      })
    },
    (err) => console.log('err: ', err),
    () => console.log('Zip to CBSA caching complete'))

  csv()
    .fromStream(request.get('https://s3.amazonaws.com/peerstreet-static/engineering/zip_to_msa/zip_to_cbsa.csv'))
    .subscribe((json)=>{
      return new Promise((resolve, reject)=>{
        client.set('ZIP-' + json.ZIP, json.CBSA)
        resolve()
      })
    },
    (err) => console.log('err: ', err),
    () => console.log('CBSA to MSA caching complete'))

  console.log('Listening on ' + PORT)
})
