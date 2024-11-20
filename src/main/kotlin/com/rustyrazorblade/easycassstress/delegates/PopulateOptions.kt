package com.rustyrazorblade.easycassstress.delegates

import com.beust.jcommander.Parameter

class PopulateOptions {
    @Parameter(names = ["--populate.rate"], description = "Sets the populate rate")
    var rate: Double = 50000.0
}