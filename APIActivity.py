import requests
import spotipy

client_id = 'b6b670e62e0e484ab33099b1737325b8'
client_secret = '5217bbe8294f40a0aab688d00b74977b'
auth_url = 'https://accounts.spotify.com/api/token'

auth_response = requests.post(auth_url, {
  'grant_type': 'client_credentials',
  'client_id': client_id,
  'client_secret': client_secret
})

print(auth_response.json())

