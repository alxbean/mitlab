package main

import "fmt"
import "time"

func main(){
    timer := time.NewTimer(time.Second * 1)

    go func(timer *time.Timer){
        if !timer.Stop(){
            <-timer.C
        }
        ok := timer.Reset(time.Second * 10)
        fmt.Println(ok)
    }(timer)

    <-timer.C

    fmt.Println("Hello")
}
