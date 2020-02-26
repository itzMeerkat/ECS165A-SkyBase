def merge_tail_page(tail_pids, bufferpool, page_directory):
    while len(tail_pids) > 0:
        t_pages = tail_pids # copy for concurrency (pids)
        base_page_copy = bufferpool.get_base_pages(t_pages) #{pid:page}
        # Go through all rids in base pages, build a hashmap
        seen_latest = {}

        false_count = 0
        for p in base_page_copy:
            l = p.num_records
            for i in range(1, l+1):
                _, rid = p.read(i, True)
                if page_directory[rid].get_indirection() == rid:
                    seen_latest[rid] = True
                else:
                    seen_latest[rid] = False
                    false_count += 1
        
        for _pid in t_pages:
            tail_pids.remove(_pid) # modify the original one
            page = bufferpool.get(_pid)
            l = page.num_records
            col_index = _pid >> 56
            for i in range(l+1,0,-1):
                val, rid = page.read(i, True)
                if not rid in seen_latest:
                    seen_latest[rid] = True

                    ind_rid = page_directory[rid].get_indirection()
                    _r = page_directory[ind_rid]
                    col_loc = _r.locations[col_index + 4]

                    base_page_copy[col_loc[0]].inplace_update(col_loc[1], val)
                    false_count -= 1
                

                if false_count <= 0:
                    break


            if false_count <= 0:
                break
            
        

        # TODO: Update page directory and deallocate old base pages
        return base_page_copy # This is new base pages
