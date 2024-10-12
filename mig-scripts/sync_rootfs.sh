#user=root
host=$1
rootfs=$2

inotifywait -mrq --timefmt '%Y%m%d %H:%M' --format '%T %w%f%e' -e modify,delete,create,attrib,move $rootfs \
| while read files;do
    rsync -ahvzP --delete --timeout=100 $rootfs/ root@$host:$rootfs/
    echo "${files} was rsynced" >& ./logs/rsync_filename.log
done