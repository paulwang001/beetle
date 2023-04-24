use crate::Client;
use crate::ContactsView;
use crate::ClientResult;
use crate::sled_db::contacts::ContactsDb;

impl Client {
    pub(crate) fn get_contacts_view_by_id(&self, id: u64) -> ClientResult<Option<ContactsView>> {
        let tree = Self::open_contact_tree(self.db())?;
        
        let tag = match 
        tree.get(format!("TAG-{id}"))?
            .and_then(|x| String::from_utf8(x.to_vec()).ok())
        {
            Some(x) => x,
            None => return Ok(None)
        };

        let addition_tag = tree.get(format!("TAG-ADDITION-{id}"))
                .ok()
                .flatten()
                .and_then(|x| String::from_utf8(x.to_vec()).ok())
                .unwrap_or_default();

        let c_type = Self::get_contacts_type(self.db(), id).map(|x| x as u8).unwrap_or(3);

        
        Ok(
            Some(
                ContactsView{
                    did: id,
                    tag,
                    addition_tag,
                    c_type
                }
            )
        )
    }

    pub(crate) fn exits_contacts_by_id(&self, id: u64) -> ClientResult<bool> {
        let tree = Self::open_contact_tree(self.db())?;
        Ok(
            tree.get(format!("TAG-{id}"))?.is_some()
        )
    }
}