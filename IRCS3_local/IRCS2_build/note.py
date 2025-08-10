Untuk ul
saya udah tidak pakai 'summary_ul_dv_final','summary_full_stat_total', 'summary_diff_total' jadi tolong pada row 4 diganti menjadi rumus excel sum dari kolom itu sendiri dari row 11 sampai datanya habis (habisnya dinilai dari berapa banyak kolom D11 hingga paling bawah) 
mulai kolomnya dari E sampai L dan number formatnya sama
Untuk kolom M sampai P akan menjadi 
kolom M = E4-I4
N = F4-J4
O  = G4-K4
P= H4-L4
untuk row 6 diganti menjadi rumus excel sum dari kolom itu sendiri dari row 11 sampai datanya habis (habisnya dinilai dari berapa banyak kolom D11 hingga paling bawah) jika nama pada B11 hingga habis depannya terdapat U
mulai kolomnya dari E sampai P dan number formatnya sama
lalu untuk 'result' dan 'table1' udah gak saya pakai sebagai gantinya akan digunakan rumus excel dengan perhitungan
kolom M11 = E11-I11
N 11 = F11-J11
O11  = G11-K11
P11 = H11-L11
lakukan tersebut sampai row habis dan number formatnya sama
lalu untuk 'result_percent','merged_2', dan 'table2' udah saya gak pakai diganti dengan rumus excel dengan perhitungan
kolom Q11 = IFERROR(M11/I11;0)
 R11 = IFERROR(N11/IJ11;0)
 S11 = IFERROR(O11/K11;0)
 T11 = IFERROR(P11/L11;0)
dibuat dalam format percentage excel dan sampai rownya habis
'Different_Percentage' dan 'Different_Percentage_of_Checking_Result_to_Raw_Data' udah gak saya pakai diganti dengan rumus excel dengan perhitungan berikut pada Diff Percentage
pada kolom Q = IFERROR(ROUND(M4/I4 * 100;1);0)
R = IFERROR(ROUND(N4/J4 * 100;1);0)
S = IFERROR(ROUND(O4/K4 * 100;1);0)
T = IFERROR(ROUND(P4/L4 * 100;1);0)
dibuat dalam format percentage 
nanti hasil yang ditampilkan untuk ul hanya sampai merged pada ul

untuk trad
saya udah tidak pakai 'summary_trad_dv_final','summary_full_stat_total', 'summary_diff_total_input' ,'sum_diff_aztrad_otuput','result','merged_2','total','sum_diff_aztrad', 'merged_3'
jadi tolong pada row 4 diganti menjadi rumus excel sum dari kolom itu sendiri dari row 11 sampai datanya habis (habisnya dinilai dari berapa banyak kolom D11 hingga paling bawah) 
mulai kolomnya dari E sampai L dan number formatnya sama
kolom M = E4-I4
N = F4-J4
O  = G4-K4
P= H4-L4
untuk row 6 diganti menjadi rumus excel sum dari kolom itu sendiri dari row 11 sampai datanya habis (habisnya dinilai dari berapa banyak kolom D11 hingga paling bawah) jika nama pada B11 hingga habis depannya terdapat C dan namanya WPCI77
mulai kolomnya dari E sampai P dan number formatnya sama
rumus excel dengan perhitungan
kolom M11 = E11-I11
N 11 = F11-J11- mencari nilai pada sheet'SUMMARY_CAMPAIGN' kolom H jika nama pada D11 sama dengan nama di sheet 'SUMMARY_CAMPAIGN' kolom B3 hingga terakhir 
O11  = G11-K11 + mencari nilai pada sheet'BSI' kolom C jika nama pada D11 sama dengan nama di sheet 'SUMMARY_CAMPAIGN' kolom A2 hingga terakhir 
P11 = H11-L11
lakukan tersebut sampai row habis dan number formatnya sama
lalu untuk 'result_percent','merged_3'udah saya gak pakai diganti dengan rumus excel dengan perhitungan
kolom Q11 = IFERROR(M11/I11;0)
 R11 = IFERROR(N11/IJ11;0)
 S11 = IFERROR(O11/K11;0)
 T11 = IFERROR(P11/L11;0)
dibuat dalam format percentage excel dan sampai rownya habis
'Different_Percentage' dan udah gak saya pakai diganti dengan rumus excel dengan perhitungan berikut pada Diff Percentage
pada kolom Q = IFERROR(ROUND(M6/I4 * 100;1);0)
R = IFERROR(ROUND(N6/J4 * 100;1);0)
S = IFERROR(ROUND(O6/K4 * 100;1);0)
T = IFERROR(ROUND(P6/L4 * 100;1);0)
dibuat dalam format percentage 
untuk trad hanya akan ditampilkan hasil data merged pada tabel trad
jadi ul dan trad memiliki tabel merged sendiri sendiri

lalu untuk sheet 'CONTROL_2_SUMMARY' semuanya sama dengan judul dan formatnya tapi akan dibuat menjadi pada kolom B4 "UL_IDR" B5"UL_USD" B6"TRAD_IDR" B7"TRAD_USD" lalu untuk kolom C sampai J maka akan seperti ini
untuk UL_IDR maka akan mengambil
kolom C = sum E11: sampai habis Summary_Checking_UL dengan nama pada kolom D11 sampai habis Summary_Checking_UL yang ada IDR nya gitu terus sampai kolom J dengan gerak 1 pada pada kolom Summary_Checking_UL