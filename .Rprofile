if(system.file(package="here")!="")
  Sys.setenv(R_CONFIG_FILE = here::here("tests/config-tessireport.yml"))

user_profile <- file.path(Sys.getenv("R_USER"),".Rprofile")
if(file.exists(user_profile))
  source(user_profile)

if(system.file(package="tinytex")!="")
  tinytex::install_tinytex()
