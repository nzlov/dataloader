# dataloader

Requires golang 1.18+ for generics support.

inspired by https://github.com/vektah/dataloaden.

The intended use is in graphql servers, to reduce the number of queries being sent to the database. These dataloader objects should be request scoped and short lived. They should be cheap to create in every request even if they dont get used.

