
def get_period(created_at):
    datetime = created_at.split()
    weekday = datetime[0]
    month = datetime[1]
    day = datetime[2]
    time = datetime[3]
    year = datetime[5]
    hour = time.split(":")[0]
    return [weekday,month,day,hour,year]
