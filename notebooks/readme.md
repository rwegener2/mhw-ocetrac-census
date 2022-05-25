Processing is broken down into the steps:
1. compute climatology / threshold. Most computationally intensive. Development of code spread accross 3 scripts in oliver_mhw_code, final code is in halo2_scripts.
2. compute anomaly/hot water. This is the next step of preprocessing for ocetrac. Single notebook in geopolar-ocetrac.
3. Run ocetrac. Single notebook in geopolar-ocetrac.

Other subfolders in this directory are:
- rechunker: an experiment trying out rechunker converting some local geopolar from large-in-space to large-in-time
