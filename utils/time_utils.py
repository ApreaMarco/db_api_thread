from time import process_time as p_now
from time import time as now


def measureTime(tupleTime=None):
    extTime = now()
    cpuExtTime = p_now()

    if tupleTime is None:
        firstExtTime = extTime
        firstCpuExtTime = cpuExtTime
    else:
        firstExtTime = tupleTime[0][0]
        firstCpuExtTime = tupleTime[1][0]

    return (firstExtTime, extTime), (firstCpuExtTime, cpuExtTime)


def timeDeltaString(start, end):
    delta = end - start

    hours = int(delta // 3600)
    delta = delta - hours * 3600
    minutes = int(delta // 60)
    delta = delta - minutes * 60
    seconds = int(delta)
    delta = delta - seconds
    milliseconds = int(delta * 1000)

    return f"{hours}:{minutes:0<2d}:{seconds:0<2d}.{milliseconds:0<3d}"


def measureTimeString(tupleTime=None):
    if tupleTime is not None:
        tupleTime = measureTime(tupleTime)

    extTime = timeDeltaString(tupleTime[0][0], tupleTime[0][1])
    cpuExtTime = timeDeltaString(tupleTime[1][0], tupleTime[1][1])

    return f"Execution Time: {extTime}, CPU Execution Time: {cpuExtTime}"


def main():
    tupleTime = measureTime()
    print(f"time: {measureTimeString(tupleTime)}")


if __name__ == '__main__':
    main()
