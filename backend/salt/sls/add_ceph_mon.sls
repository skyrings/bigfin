{% if pillar.get('skyring') %}
{% set this_node = grains['id'] %}
{% set cluster_name = pillar['skyring'][this_node]['cluster_name'] %}
{% set mon_id = pillar['skyring'][this_node]['mon_id'] %}
{% set mon_name = pillar['skyring'][this_node]['mon_name'] %}
{% set port = pillar['skyring'][this_node].get('port') %}

skyring_cluster_name:
  grains.present:
    - value: {{ cluster_name }}

skyring_node_type:
  grains.present:
    - value: mon

skyring_mon_id:
  grains.present:
    - value: {{ mon_id }}

skyring_mon_name:
  grains.present:
    - value: {{ mon_name }}

/etc/ceph/{{ cluster_name }}.conf:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/{{ cluster_name }}.conf
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/etc/ceph/{{ cluster_name }}.client.admin.keyring:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/client.admin.keyring
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/etc/ceph/{{ cluster_name }}.mon.key:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/mon.key
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/etc/ceph/{{ cluster_name }}.mon.map:
  file.managed:
    - source: salt://skyring/conf/ceph/{{ cluster_name }}/mon.map
    - user: root
    - group: root
    - mode: 644
    - makedirs: True
    - show_diff: False

/var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}:
  file.directory:
    - user: root
    - group: root
    - mode: 755
    - makedirs: True

populate_monitor:
  cmd.run:
    - name: ceph-mon --cluster {{ cluster_name }} --mkfs -i {{ mon_id }} --monmap /etc/ceph/{{ cluster_name }}.mon.map --keyring /etc/ceph/{{ cluster_name }}.mon.key
    - require:
      - file: /etc/ceph/{{ cluster_name }}.conf
      - file: /etc/ceph/{{ cluster_name }}.client.admin.keyring
      - file: /etc/ceph/{{ cluster_name }}.mon.key
      - file: /etc/ceph/{{ cluster_name }}.mon.map
      - file: /var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}

/var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}/done:
  file.touch:
  - require:
    - cmd: populate_monitor

/var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}/sysvinit:
  file.touch:
  - require:
    - file: /var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}/done

start_ceph_mon:
  cmd.run:
    - name: service ceph --cluster {{ cluster_name }} start mon.{{ mon_id }}
    - require:
      - file: /var/lib/ceph/mon/{{ cluster_name }}-{{ mon_id }}/sysvinit

{% if not pillar['skyring'].get('mon_bootstrap') %}
add_monitor:
  cmd.run:
    - name: ceph --cluster {{ cluster_name }} mon add {{ mon_id }} {{ pillar['skyring'][this_node]['public_ip'] }}
    - require:
      - cmd: start_ceph_mon
{% endif %}
{% endif %}
