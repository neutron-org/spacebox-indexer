# How to generate/regenerate files

From this directory (user_files):

- skip-assets.json
  - `wget -qO - https://app.neutron.org/api/assets | jq '.chain_to_assets_map["neutron-1"].assets' > mainnet/skip-assets.json`
  - `wget -qO - https://app-neutron-testnet.vercel.app/api/assets | jq '.chain_to_assets_map["neutron-1"].assets' > testnet/skip-assets.json`
