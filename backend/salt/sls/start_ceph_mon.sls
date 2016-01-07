{% set cluster_name = grains['skyring_cluster_name'] %}
{% set mon_id = grains['skyring_mon_id'] %}

start_ceph_mon:
  cmd.run:
    - name: service ceph --cluster {{ cluster_name }} start mon.{{ mon_id }}

start_ceph_calamari:
  cmd.run:
    - name: service supervisord restart; supervisorctl restart all
