{% if pillar.get('skyring') %}
{% set this_node = grains['id'] %}
{% set cluster_name = pillar['skyring'][this_node]['cluster_name'] %}
{% set cluster_id = pillar['skyring'][this_node]['cluster_id'] %}
{% set devices = pillar['skyring'][this_node]['devices'] %}

skyring_cluster_name:
  grains.present:
    - value: {{ cluster_name }}

skyring_node_type:
  grains.present:
    - value: osd

/etc/ceph/{{ cluster_name }}.conf:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/{{ cluster_name }}.conf
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/var/lib/ceph/bootstrap-osd/{{ cluster_name }}.keyring:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/{{ cluster_name }}.keyring
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/var/lib/ceph/osd:
  file.directory:
    - user: root
    - group: root
    - mode: 755
    - makedirs: True

{% for osd, fs_type in devices.iteritems() %}
prepare_{{ osd }}:
  cmd.run:
    - name: ceph-disk prepare --cluster {{ cluster_name }} --cluster-uuid {{ cluster_id }} --fs-type {{ fs_type }} --zap-disk {{ osd }}
    - onlyif: lsblk --nodeps -n -o TYPE {{ osd }} | grep -q 'disk'
{% endfor %}

{% endif %}
