require 'ruby-debug'

#  +------------------+----------+
#  | ieid             | size     |
#  +------------------+----------+
#  | E20070101_AAAAAX |     0.20 |
#  | E20060914_AAAACL |     0.44 |
#  | E20081121_AAAAIS |    58.76 |
#  | E20090929_AAAAEZ |   156.69 |
#  | E20100121_AAAAAA |   891.21 |
#  | E20090905_AAAAAU |  3546.79 |
#  | E20090925_AAAABG |  6396.32 |
#  | E20091219_AAAAJU |  7238.40 |
#  | E20060710_AAAAAI | 18206.12 |
#  +------------------+----------+

complete = [
  ['E20051213_AAAAAA', 'UWF', 'ETD'],
  ['E20060108_AAAAAA', 'UCF', 'ETD'],
  ['E20060108_AAAAAE', 'UCF', 'ETD'],
  ['E20060113_AAAAAA', 'UCF', 'ETD'],
  ['E20060117_AAAABE', 'UWF', 'WFPA'],
  ['E20060306_AAAAAH', 'UCF', 'ETD'],
  ['E20060405_AAAAAE', 'UCF', 'ETD'],
  ['E20060413_AAAAYA', 'UWF', 'WFPA'],
  ['E20060419_AAAAAB', 'UCF', 'ETD'],
  ['E20070101_AAAAAX', 'UCF', 'FHP'],
  ['E20081121_AAAAIS', 'FDA', 'FDA'],
]

too_big = [
  ['E20060710_AAAAAI', 'UCF', 'ETD'],
  ['E20091219_AAAAJU', 'UF', 'UFDC'],
  ['E20090925_AAAABG', 'UNF', 'FHP'],
  ['E20090905_AAAAAU', 'UCF', 'FHP'],
  ['E20100121_AAAAAA', 'FAU', 'YCB']
]

_404ed = [
  ['E20060914_AAAACL', 'UF', 'ETD'],
]

errors = [
  ['E20090929_AAAAEZ', 'USF', 'FHP'],
]

# migrate them over
ps = too_big + errors + _404ed

require 'daitss'
include Daitss
load_archive

ps.each do |id, a, p|
  path = File.join(archive.workspace.path, id)

  system "bin/migrate_ieid #{id} #{a} #{p}" unless Package.get(id)
  Wip.make path, :d1refresh unless archive.workspace[id]

  wip = Wip.new path
  puts "starting #{id}"
  wip.d1refresh

  if Package.get(id).aip.datafile_count
    puts 'ok'
  else
    puts 'ng'
  end

  puts
end

# make a d1 refresh wip & start it
