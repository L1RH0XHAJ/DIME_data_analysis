cd "/Users/matte/Documents/RESEARCH/ONGOING/DONATIONS/f2/campaign contributions/New/DATA/DIME/SP500/MOD"

use sp500_contributions, clear
use rec, clear
drop if length(cycle) > 4
drop if cycle==""
destring cycle, replace
 
merge m:m bonicarid cycle using sp500_contributions

drop if _merge!=3
drop _merge

keep if seat=="federal:house"
drop comtype
drop election
drop fecyear
drop bonicacid
drop lname ffname fname mname title suffix feccandstatus recipienttype igcat candid fecid nid dimecid ticker  recipientparty recipientname

rename totaldisbursements rec_total_spent
lab var rec_total_spent "Recipient's total disbursements"

rename totalreceipts rec_total_collected
lab var rec_total_collected "Recipient's Total receipts"

rename state rec_state

lab var rec_state "Recipient State"

rename pwinner rec_primary_el_outcome
lab var rec_primary_el_outcome "Recipient's primary election outcome"

rename name rec_name
lab var rec_name "Recipient's name"

gen rec_party = ""
replace rec_party = "D" if party == "100"
replace rec_party = "R" if party == "200"
replace rec_party = "Other" if party != "100" & party != "200"
drop party
lab var rec_party "Recipient's party"

rename icostatus rec_inc_status
lab var rec_inc_status "Recipient's incumbency status"

rename seat rec_seat 
lab var rec_seat "Recipient's type of race"

rename gwinner rec_gen_el_outcome
lab var rec_gen_el_outcome "Recipient's general election outcome"

rename longitude cont_longitude
lab var cont_longitude "Contributor's longitude"

rename latitude cont_latitude
lab var cont_latitude "Contributor's latitude"

rename district rec_district
lab var rec_district "Recipient's district"

rename distcyc rec_dist_yc
lab var rec_dist_yc "Recipient's year_district"

lab var date "Date"
lab var cycle "Cycle"

rename corpname cont_corporation_name
lab var cont_corporation_name "Contributor's corporation"

rename contributorzipcode cont_zip_code 
lab var cont_zip_code "Contributor's zip code"

rename contributorstate cont_state
lab var cont_state "Contributor's state"

rename contributoroccupation cont_occ
lab var cont_occ "Contributor's occupation"

rename contributorname cont_name
lab var cont_name "Contributor's name'"

lab var amount "Amount"

rename candgender rec_gender
lab var rec_gender "Recipient's gender'"


rename bonicarid rec_id
lab var rec_id "Recipient ID'"

rename transactionid transaction_id
lab var transaction_id "Transaction ID'"



save merge_sp_500_cleaned, replace



//everything mached except for vote shares variables
use merge_sp_500_cleaned, clear


gen length = ceil(log10(primvotepct)) if primvotepct > 1 & !missing(primvotepct)

replace primvotepct = primvotepct / (10^length) if primvotepct > 1 & !missing(primvotepct)

rename primvotepct rec_prim_vote_share 
lab var rec_prim_vote_share "Recipient primary vote share"

drop length

replace genvotepct =. if genvotepct<0
replace genvotepct =. if genvotepct>100


replace genvotepct = genvotepct*100 if genvotepct<1 & genvotepct>0

replace genvotepct = 100 if genvotepct==1

rename genvotepct rec_gen_vote_share 
lab var rec_gen_vote_share "Recipient general vote share"

replace districtpresvs =  districtpresvs*100 if  districtpresvs<1
replace districtpresvs =  districtpresvs/10 if  districtpresvs>100

rename districtpresvs dist_dem_share
lab var dist_dem_share "District vote share of Dem presidential candidate last election"

save merge_sp_500_cleaned_2, replace

// run from here to work with merged dataset
use final_sp_500, clear

//re label variables
lab var rec_total_spent "Recipient's total disbursements"
lab var rec_total_collected "Recipient's Total receipts"
lab var rec_total_collected "Recipient's Total receipts"
lab var rec_state "Recipient State"
lab var rec_primary_el_outcome "Recipient's primary election outcome"
lab var rec_name "Recipient's name"
lab var rec_party "Recipient's party"
lab var rec_inc_status "Recipient's incumbency status"
lab var rec_seat "Recipient's type of race"
lab var rec_gen_el_outcome "Recipient's general election outcome"
lab var cont_longitude "Contributor's longitude"
lab var cont_latitude "Contributor's latitude"
lab var rec_district "Recipient's district"
lab var rec_dist_yc "Recipient's year_district"
lab var date "Date"
lab var cycle "Cycle"
lab var cont_corporation_name "Contributor's corporation"
lab var cont_zip_code "Contributor's zip code"
lab var cont_state "Contributor's state"
lab var cont_occ "Contributor's occupation"
lab var cont_name "Contributor's name'"
lab var amount "Amount"
lab var rec_gender "Recipient's gender'"
lab var rec_id "Recipient ID'"
lab var transaction_id "Transaction ID'"
lab var rec_prim_vote_share "Recipient primary vote" 
lab var dist_dem_share "District vote share of Dem presidential candidate last election"
lab var rec_gen_vote_share "Recipient general vote share"

lab var nearest_death_unexpected "Death incumbent unexpected or not"
lab var nearest_death_age "Death incumbent age"
lab var days_to_nearest_death "Death incumbent distance in days to contribution"

save final_sp_500_with_labels, replace
