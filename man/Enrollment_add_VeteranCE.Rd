% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/prep_enrollment_utils.R
\name{Enrollment_add_VeteranCE}
\alias{Enrollment_add_VeteranCE}
\title{Add Veteran Coordinated Entry Date to Enrollments}
\usage{
Enrollment_add_VeteranCE(Enrollment, VeteranCE)
}
\arguments{
\item{Enrollment}{that includes Exit Data. See \code{Enrollment_add_Exit}}

\item{VeteranCE}{HUD Extra that includes Veteran Coordinated Entry data}
}
\value{
\code{(data.frame)} Enrollment with the following columns added \code{PHTrack}, \code{ExpectedPHDate}, \code{ExitAdjust}
}
\description{
Add Veteran Coordinated Entry Date to Enrollments
}
