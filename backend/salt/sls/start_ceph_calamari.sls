start_ceph_calamari:
  cmd.run:
    - name: calamari-ctl initialize; service supervisord restart; supervisorctl restart all
