alias ssubmit='spark-submit --queue ***  --master yarn --num-executors 80 --executor-cores 2'

alias rm=trash
trash()
{
  mv $@ /home/.trash/
}
~
