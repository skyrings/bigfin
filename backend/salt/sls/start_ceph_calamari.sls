start_ceph_calamari:
  cmd.run:
    - name: calamari-ctl initialize --admin-username admin --admin-password admin --admin-email skyring@redhat.com; service supervisord restart; supervisorctl restart all
