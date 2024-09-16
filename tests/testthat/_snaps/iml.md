# parse_shapley returns a human-redable version of a shapley analysis

    Code
      lapply(explanations, parse_shapley)
    Output
      [[1]]
      [1] "ticketPerfCount__365: 21.0; ticketFeeAmt__90: 1.5; ticketFeeAmt__365: 1.5"
      
      [[2]]
      [1] "ticketPerfCount__365: 21.0; ticketDiscountMemAmt__365: 160.0; ticketFeeAmt__90: 1.5"
      
      [[3]]
      [1] "ticketFeeAmt__90: 1.5; ticketDiscountMemAmt__365: 160.0; ticketPerfCount__365: 21.0"
      
      [[4]]
      [1] "ticketPerfCount__365: 21.0; ticketFeeAmt__90: 1.5; ticketDiscountMemAmt__7: 8.0"
      
      [[5]]
      [1] "ticketFeeAmt__90: 1.5; ticketPerfCount__365: 21.0; ticketFeeAmt__365: 1.5"
      
      [[6]]
      [1] "ticketPerfCount__365: 21.0; ticketPerfCount__90: 7.0; ticketFeeAmt__365: 1.5"
      
      [[7]]
      [1] "ticketPerfCount__365: 6; ticketFeeAmt__365: 6; ticketPerfCount__90: 2"
      
      [[8]]
      [1] "ticketDiscountMemAmt__365: 40; ticketPerfCount__365: 5; ticketPerfCount__90: 1"
      
      [[9]]
      [1] "ticketPerfCount__365: 6; ticketFeeAmt__365: 10; ticketFeeAmt__90: 4"
      
      [[10]]
      [1] "ticketFeeAmt__90: 4; ticketFeeAmt__365: 10; ticketPerfCount__365: 6"
      
      [[11]]
      [1] "ticketPerfCount__365: 10.0; ticketFeeAmt__365: 61.0; ticketFeeAmt__90: 1.5"
      
      [[12]]
      [1] "ticketFeeAmt__365: 22.0; ticketFeeAmt: 65.5; ticketFeeAmt__90: 6.0"
      
      [[13]]
      [1] "timestamp: 1,002 days; ticketFeeAmt__365: 9; ticketFilmCount: 12"
      
      [[14]]
      [1] "timestamp: 984 days; ticketFeeAmt__365: 4.5; ticketFilmCount: 6.0"
      
      [[15]]
      [1] "timestamp: 1,000 days; email_send_count: 153; email_open_timestamp_min: 951 days"
      
      [[16]]
      [1] "ticketPerfCount__365: 7; ticketTimestampLast: 0 days; ticketPerfCount: 13"
      
      [[17]]
      [1] "timestamp: 986 days; email_click_timestamp_min: 427 days; email_send_timestamp_max: 1,165 days"
      
      [[18]]
      [1] "timestamp: 998 days; email_open_timestamp_max: 500 days; email_open_count__30: -26"
      
      [[19]]
      [1] "timestamp: 884 days; email_open_timestamp_max: 411 days; address_median_income_level: 71,464"
      
      [[20]]
      [1] "ticketPerfCount__365: 6; ticketFilmCount__30: 8; ticketPerfCount: 13"
      
      [[21]]
      [1] "timestamp: 1,059 days; email_open_timestamp_max: 603 days; email_send_timestamp_max: 1,177 days"
      
      [[22]]
      [1] "timestamp: 1,037 days; email_click_timestamp_max: 962 days; email_click_timestamp_min: 925 days"
      
      [[23]]
      [1] "timestamp: 838 days; email_send_count: 400; email_click_count__90: 1"
      
      [[24]]
      [1] "timestamp: 839 days; email_click_count: 2; address_mean_income_level: 192,126"
      
      [[25]]
      [1] "timestamp: 840 days; address_median_income_level: 109,740; email_send_count__30: 32"
      

